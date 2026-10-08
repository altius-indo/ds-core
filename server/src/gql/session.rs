//! A client session: parses GQL and runs it, either inside an explicit transaction
//! (`START TRANSACTION` ... `COMMIT` / `ROLLBACK`, GT01) or as an auto-commit statement.
//!
//! Auto-commit statements go through the internal retry (REQ-0015): a statement whose
//! transaction hits a serialization conflict is re-run from scratch at a fresh snapshot,
//! because none of its results reached the client. Statements inside an explicit
//! transaction are never retried; the conflict goes back to the client.

use std::collections::BTreeMap;

use super::ast::Statement;
use super::exec::{Executor, QueryResult};
use super::parse;
use crate::graph::store::Graph;
use crate::graph::value::Value;
use crate::txn::coordinator::{Txn, TxnClient};
use crate::txn::error::{DsError, ErrorCode};
use crate::txn::retry::{RetryPolicy, autocommit};

pub struct Session {
    client: TxnClient,
    graph: Graph,
    txn: Option<Txn>,
    read_only: bool,
    pub retry: RetryPolicy,
}

impl Session {
    pub fn new(client: TxnClient, graph: Graph) -> Self {
        Self {
            client,
            graph,
            txn: None,
            read_only: false,
            retry: RetryPolicy::default(),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    pub async fn execute(
        &mut self,
        gql: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryResult, DsError> {
        let stmt = parse(gql)?;
        match stmt {
            Statement::StartTransaction { read_only } => {
                if self.txn.is_some() {
                    return Err(DsError::new(
                        ErrorCode::InvalidConfiguration,
                        "a transaction is already open",
                    ));
                }
                self.txn = Some(self.client.begin().await?);
                self.read_only = read_only;
                Ok(QueryResult::default())
            }
            Statement::Commit => {
                let txn = self.txn.take().ok_or_else(|| {
                    DsError::new(ErrorCode::InvalidConfiguration, "no transaction is open")
                })?;
                txn.commit().await?;
                Ok(QueryResult::default())
            }
            Statement::Rollback => {
                // Nothing is written before commit; dropping the transaction discards it.
                self.txn.take();
                Ok(QueryResult::default())
            }
            Statement::Query(q) => {
                if let Some(txn) = self.txn.as_mut() {
                    let res = Executor::new(&self.graph, params).run(txn, &q).await;
                    if let Ok(r) = &res
                        && self.read_only
                        && r.mutations > 0
                    {
                        self.txn.take();
                        return Err(DsError::new(
                            ErrorCode::InvalidConfiguration,
                            "the transaction is READ ONLY; it has been rolled back",
                        ));
                    }
                    if res.is_err() {
                        // A failed statement aborts the explicit transaction.
                        self.txn.take();
                    }
                    return res;
                }
                let graph = self.graph.clone();
                let done = autocommit(&self.client, self.retry, |mut txn| {
                    let (graph, q) = (graph.clone(), q.clone());
                    async move {
                        let r = Executor::new(&graph, params).run(&mut txn, &q).await?;
                        Ok((r, txn))
                    }
                })
                .await?;
                Ok(done.value)
            }
        }
    }
}
