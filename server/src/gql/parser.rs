//! Hand-written recursive-descent GQL parser with precedence climbing for expressions
//! (design/gql-parser.md). It accepts the v1 subset in docs/gql-conformance.toml and rejects
//! recognised-but-unsupported constructs with the ISO/IEC 39075 feature they need
//! (REQ-0003 AC3), and SQL with a "not GQL" error.

// reqforge: implements REQ-0003

use super::ast::*;
use super::error::{ParseError, feature_name};
use super::lexer::{Tok, Token, lex};
use crate::graph::value::{Decimal128, Timestamp, Value};

pub fn parse(src: &str) -> Result<Statement, ParseError> {
    let toks = lex(src).map_err(|e| ParseError::Syntax {
        at: e.at,
        message: e.message,
    })?;
    let mut p = Parser { toks, pos: 0 };
    let stmt = p.statement()?;
    p.eat_punct(";");
    if !matches!(p.peek(), Tok::Eof) {
        return Err(p.unexpected("end of statement"));
    }
    Ok(stmt)
}

struct Parser {
    toks: Vec<Token>,
    pos: usize,
}

type R<T> = Result<T, ParseError>;

impl Parser {
    // ------------------------------------------------------------------ token helpers

    fn peek(&self) -> &Tok {
        &self.toks[self.pos].tok
    }

    fn peek_at(&self, n: usize) -> &Tok {
        &self.toks[(self.pos + n).min(self.toks.len() - 1)].tok
    }

    fn at(&self) -> usize {
        self.toks[self.pos].at
    }

    fn bump(&mut self) -> Tok {
        let t = self.toks[self.pos].tok.clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn is_kw_at(&self, n: usize, kw: &str) -> bool {
        matches!(self.peek_at(n), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> R<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(self.unexpected(kw))
        }
    }

    fn is_punct(&self, p: &str) -> bool {
        matches!(self.peek(), Tok::Punct(q) if *q == p)
    }

    fn is_punct_at(&self, n: usize, p: &str) -> bool {
        matches!(self.peek_at(n), Tok::Punct(q) if *q == p)
    }

    fn eat_punct(&mut self, p: &str) -> bool {
        if self.is_punct(p) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, p: &str) -> R<()> {
        if self.eat_punct(p) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("'{p}'")))
        }
    }

    fn unexpected(&self, expected: &str) -> ParseError {
        ParseError::Syntax {
            at: self.at(),
            message: format!("expected {expected}, found {}", self.peek()),
        }
    }

    fn unsupported(&self, feature: &'static str) -> ParseError {
        ParseError::Unsupported {
            at: self.at(),
            feature,
            name: feature_name(feature),
        }
    }

    fn ident(&mut self) -> R<String> {
        match self.bump() {
            Tok::Ident(s) | Tok::Quoted(s) => Ok(s),
            other => {
                self.pos -= 1;
                Err(ParseError::Syntax {
                    at: self.at(),
                    message: format!("expected an identifier, found {other}"),
                })
            }
        }
    }

    // ------------------------------------------------------------------ statements

    fn statement(&mut self) -> R<Statement> {
        self.reject_sql()?;
        if self.eat_kw("START") {
            self.expect_kw("TRANSACTION")?;
            let mut read_only = false;
            if self.eat_kw("READ") {
                if self.eat_kw("ONLY") {
                    read_only = true;
                } else {
                    self.expect_kw("WRITE")?;
                }
            }
            return Ok(Statement::StartTransaction { read_only });
        }
        if self.eat_kw("COMMIT") {
            self.eat_kw("WORK");
            return Ok(Statement::Commit);
        }
        if self.eat_kw("ROLLBACK") {
            self.eat_kw("WORK");
            return Ok(Statement::Rollback);
        }
        if self.is_kw("SESSION") {
            return Err(if self.is_kw_at(1, "RESET") {
                self.unsupported("GS08")
            } else {
                self.unsupported("GS03")
            });
        }
        if self.is_kw("CREATE") || self.is_kw("DROP") {
            return Err(if self.is_kw_at(1, "SCHEMA") {
                self.unsupported("GC01")
            } else if self.is_kw_at(1, "PROPERTY") || self.is_kw_at(2, "TYPE") {
                self.unsupported("GG03")
            } else {
                self.unsupported("GC04")
            });
        }
        Ok(Statement::Query(self.query()?))
    }

    /// SQL is a common mistake; say so instead of reporting a GQL syntax error.
    fn reject_sql(&self) -> R<()> {
        let sql = (self.is_kw("SELECT") && !self.is_punct_at(1, "("))
            || (self.is_kw("INSERT") && self.is_kw_at(1, "INTO"))
            || (self.is_kw("DELETE") && self.is_kw_at(1, "FROM"))
            || (self.is_kw("UPDATE")
                && matches!(self.peek_at(1), Tok::Ident(_))
                && self.is_kw_at(2, "SET"))
            || (self.is_kw("CREATE") && (self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "INDEX")))
            || (self.is_kw("ALTER") && self.is_kw_at(1, "TABLE"));
        if sql {
            return Err(ParseError::NotGql {
                at: self.at(),
                message: "this looks like SQL; DS-CORE accepts GQL (ISO/IEC 39075), e.g. MATCH (n) RETURN n".into(),
            });
        }
        Ok(())
    }

    fn query(&mut self) -> R<Query> {
        let graph = if self.eat_kw("USE") {
            Some(self.ident()?)
        } else {
            None
        };
        let mut clauses = Vec::new();
        loop {
            self.reject_composite()?;
            if matches!(self.peek(), Tok::Eof) || self.is_punct(";") {
                break;
            }
            if self.is_kw("USE") {
                return Err(self.unsupported("GT03"));
            }
            clauses.push(self.clause()?);
            // RETURN ends a linear query; only composite-query keywords may follow it, and
            // those are unsupported.
            if matches!(clauses.last(), Some(Clause::Return(_))) {
                self.reject_composite()?;
                break;
            }
        }
        if clauses.is_empty() {
            return Err(self.unexpected("a query clause"));
        }
        Ok(Query { graph, clauses })
    }

    fn reject_composite(&self) -> R<()> {
        for (kw, f) in [
            ("UNION", "GQ03"),
            ("EXCEPT", "GQ04"),
            ("INTERSECT", "GQ06"),
            ("OTHERWISE", "GQ02"),
            ("NEXT", "GQ20"),
            ("YIELD", "GQ19"),
        ] {
            if self.is_kw(kw) {
                return Err(self.unsupported(f));
            }
        }
        Ok(())
    }

    fn clause(&mut self) -> R<Clause> {
        if self.is_kw("OPTIONAL") {
            self.bump();
            if self.is_punct("{") || self.is_punct("(") {
                return Err(self.unsupported("GQ21"));
            }
            return self.match_clause(true);
        }
        if self.is_kw("MATCH") {
            return self.match_clause(false);
        }
        if self.eat_kw("FILTER") {
            self.eat_kw("WHERE");
            return Ok(Clause::Filter(self.expr()?));
        }
        if self.eat_kw("LET") {
            let mut binds = Vec::new();
            loop {
                let v = self.ident()?;
                self.expect_punct("=")?;
                binds.push((v, self.expr()?));
                if !self.eat_punct(",") {
                    break;
                }
            }
            return Ok(Clause::Let(binds));
        }
        if self.eat_kw("FOR") {
            let var = self.ident()?;
            self.expect_kw("IN")?;
            let list = self.expr()?;
            if self.is_kw("WITH") {
                if self.is_kw_at(1, "ORDINALITY") {
                    return Err(self.unsupported("GQ11"));
                }
                return Err(self.unsupported("GQ24"));
            }
            return Ok(Clause::For { var, list });
        }
        if self.eat_kw("INSERT") {
            return Ok(Clause::Insert(self.path_list()?));
        }
        if self.eat_kw("SET") {
            let mut items = Vec::new();
            loop {
                items.push(self.set_item()?);
                if !self.eat_punct(",") {
                    break;
                }
            }
            return Ok(Clause::Set(items));
        }
        if self.eat_kw("REMOVE") {
            let mut items = Vec::new();
            loop {
                let var = self.ident()?;
                if self.eat_punct(":") || self.eat_kw("IS") {
                    items.push(RemoveItem::Label {
                        var,
                        label: self.ident()?,
                    });
                } else {
                    self.expect_punct(".")?;
                    items.push(RemoveItem::Property {
                        var,
                        key: self.ident()?,
                    });
                }
                if !self.eat_punct(",") {
                    break;
                }
            }
            return Ok(Clause::Remove(items));
        }
        if self.is_kw("DETACH") || self.is_kw("NODETACH") || self.is_kw("DELETE") {
            let detach = self.eat_kw("DETACH");
            self.eat_kw("NODETACH");
            self.expect_kw("DELETE")?;
            let mut items = Vec::new();
            loop {
                if self.is_kw("VALUE") || self.is_punct("{") {
                    return Err(self.unsupported("GD03"));
                }
                let at = self.at();
                let e = self.expr()?;
                // GD04 (arbitrary expressions) is unsupported: DELETE takes variables.
                if !matches!(e, Expr::Var(_)) {
                    return Err(ParseError::Syntax {
                        at,
                        message: "DELETE takes element variables in this release".into(),
                    });
                }
                items.push(e);
                if !self.eat_punct(",") {
                    break;
                }
            }
            return Ok(Clause::Delete { detach, items });
        }
        if self.is_kw("RETURN") {
            return Ok(Clause::Return(self.return_clause()?));
        }
        if self.is_kw("CALL") {
            return Err(if self.is_punct_at(1, "{") || self.is_punct_at(1, "(") {
                self.unsupported("GP01")
            } else {
                self.unsupported("GP04")
            });
        }
        Err(self.unexpected(
            "MATCH, OPTIONAL MATCH, FILTER, LET, FOR, INSERT, SET, REMOVE, DELETE or RETURN",
        ))
    }

    fn match_clause(&mut self, optional: bool) -> R<Clause> {
        self.expect_kw("MATCH")?;
        if self.is_kw("REPEATABLE") {
            return Err(self.unsupported("G003"));
        }
        if self.is_kw("DIFFERENT") {
            return Err(self.unsupported("G002"));
        }
        if self.is_kw("KEEP") {
            return Err(self.unsupported("G006"));
        }
        let pattern = self.path_list()?;
        if self.is_kw("KEEP") {
            return Err(self.unsupported("G006"));
        }
        let filter = if self.eat_kw("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Clause::Match {
            optional,
            pattern,
            filter,
        })
    }

    fn set_item(&mut self) -> R<SetItem> {
        let var = self.ident()?;
        if self.eat_punct(":") || self.eat_kw("IS") {
            return Ok(SetItem::Label {
                var,
                label: self.ident()?,
            });
        }
        if self.eat_punct("=") {
            return Ok(SetItem::AllProperties {
                var,
                value: self.expr()?,
            });
        }
        self.expect_punct(".")?;
        let key = self.ident()?;
        self.expect_punct("=")?;
        Ok(SetItem::Property {
            var,
            key,
            value: self.expr()?,
        })
    }

    fn return_clause(&mut self) -> R<Return> {
        self.expect_kw("RETURN")?;
        let distinct = self.eat_kw("DISTINCT");
        if !distinct {
            self.eat_kw("ALL");
        }
        let items = if self.eat_punct("*") {
            None
        } else {
            let mut items = Vec::new();
            loop {
                let e = self.expr()?;
                let alias = if self.eat_kw("AS") {
                    Some(self.ident()?)
                } else {
                    None
                };
                items.push((e, alias));
                if !self.eat_punct(",") {
                    break;
                }
            }
            Some(items)
        };
        if self.is_kw("GROUP") {
            return Err(self.unsupported("GQ15"));
        }
        let mut order_by = Vec::new();
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                let expr = self.expr()?;
                let descending = if self.eat_kw("DESC") || self.eat_kw("DESCENDING") {
                    true
                } else {
                    let _ = self.eat_kw("ASC") || self.eat_kw("ASCENDING");
                    false
                };
                let nulls_first = if self.eat_kw("NULLS") {
                    if self.eat_kw("FIRST") {
                        Some(true)
                    } else {
                        self.expect_kw("LAST")?;
                        Some(false)
                    }
                } else {
                    None
                };
                order_by.push(SortKey {
                    expr,
                    descending,
                    nulls_first,
                });
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        let offset = if self.eat_kw("OFFSET") || self.eat_kw("SKIP") {
            Some(self.expr()?)
        } else {
            None
        };
        let limit = if self.eat_kw("LIMIT") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Return {
            distinct,
            items,
            order_by,
            offset,
            limit,
        })
    }

    // ------------------------------------------------------------------ patterns

    fn path_list(&mut self) -> R<Vec<Path>> {
        let mut paths = vec![self.path()?];
        while self.eat_punct(",") {
            paths.push(self.path()?);
        }
        Ok(paths)
    }

    fn path(&mut self) -> R<Path> {
        // `p = (...)`: path variables.
        if matches!(self.peek(), Tok::Ident(_)) && self.is_punct_at(1, "=") {
            return Err(self.unsupported("G004"));
        }
        self.path_prefix()?;
        let mut nodes = vec![self.node()?];
        let mut edges = Vec::new();
        loop {
            if self.is_punct("(") {
                return Err(self.unsupported("G038"));
            }
            let Some(edge) = self.edge()? else { break };
            edges.push(edge);
            nodes.push(self.node()?);
        }
        Ok(Path { nodes, edges })
    }

    fn path_prefix(&mut self) -> R<()> {
        if self.eat_kw("WALK") {
            let _ = self.eat_kw("PATH") || self.eat_kw("PATHS");
            return Ok(());
        }
        for (kw, f) in [("TRAIL", "G011"), ("SIMPLE", "G012"), ("ACYCLIC", "G013")] {
            if self.is_kw(kw) {
                return Err(self.unsupported(f));
            }
        }
        if self.is_kw("ANY") {
            return Err(if self.is_kw_at(1, "SHORTEST") {
                self.unsupported("G018")
            } else {
                self.unsupported("G016")
            });
        }
        if self.is_kw("ALL") {
            return Err(if self.is_kw_at(1, "SHORTEST") {
                self.unsupported("G017")
            } else {
                self.unsupported("G015")
            });
        }
        if self.is_kw("SHORTEST") {
            return Err(if self.is_kw_at(2, "GROUP") || self.is_kw_at(2, "GROUPS") {
                self.unsupported("G020")
            } else {
                self.unsupported("G019")
            });
        }
        Ok(())
    }

    fn node(&mut self) -> R<NodePattern> {
        self.expect_punct("(")?;
        let mut n = NodePattern::default();
        if matches!(self.peek(), Tok::Ident(_) | Tok::Quoted(_))
            && !self.is_kw("WHERE")
            && !self.is_kw("IS")
        {
            n.var = Some(self.ident()?);
        }
        if self.eat_punct(":") || self.eat_kw("IS") {
            n.labels = Some(self.label_expr()?);
        }
        if self.is_punct("{") {
            n.props = self.props()?;
        }
        if self.eat_kw("WHERE") {
            n.filter = Some(self.expr()?);
        }
        if self.is_punct("(") {
            return Err(self.unsupported("G038"));
        }
        self.expect_punct(")")?;
        Ok(n)
    }

    /// An edge pattern, or `None` if the path ends here.
    fn edge(&mut self) -> R<Option<EdgePattern>> {
        // Undirected forms: ~[..]~, ~, <~, ~>.
        if self.is_punct("~") || self.is_punct("<~") || self.is_punct("~>") {
            return Err(self.unsupported("GH02"));
        }
        let mut e = EdgePattern {
            var: None,
            labels: None,
            props: Vec::new(),
            filter: None,
            dir: Dir::Any,
            hops: None,
        };
        // Full edge patterns: -[ ]->, <-[ ]-, -[ ]-, <-[ ]->.
        let left = if self.is_punct("<-") && self.is_punct_at(1, "[") {
            self.bump();
            true
        } else if self.is_punct("-") && self.is_punct_at(1, "[") {
            self.bump();
            false
        } else {
            // Abbreviated: ->, <-, -, <->.
            e.dir = if self.eat_punct("->") {
                Dir::Right
            } else if self.eat_punct("<->") {
                Dir::Any
            } else if self.eat_punct("<-") {
                Dir::Left
            } else if self.eat_punct("-") {
                Dir::Any
            } else {
                return Ok(None);
            };
            self.quantifier(&mut e)?;
            return Ok(Some(e));
        };
        self.expect_punct("[")?;
        if matches!(self.peek(), Tok::Ident(_) | Tok::Quoted(_))
            && !self.is_kw("WHERE")
            && !self.is_kw("IS")
        {
            e.var = Some(self.ident()?);
        }
        if self.eat_punct(":") || self.eat_kw("IS") {
            e.labels = Some(self.label_expr()?);
        }
        if self.is_punct("{") {
            e.props = self.props()?;
        }
        if self.eat_kw("WHERE") {
            e.filter = Some(self.expr()?);
        }
        self.expect_punct("]")?;
        if self.is_punct("~") {
            return Err(self.unsupported("GH02"));
        }
        let right = if self.eat_punct("->") {
            true
        } else {
            self.expect_punct("-")?;
            false
        };
        e.dir = match (left, right) {
            (false, true) => Dir::Right,
            (true, false) => Dir::Left,
            _ => Dir::Any,
        };
        self.quantifier(&mut e)?;
        Ok(Some(e))
    }

    fn quantifier(&mut self, e: &mut EdgePattern) -> R<()> {
        if self.is_punct("*") || self.is_punct("+") {
            return Err(self.unsupported("G061"));
        }
        if self.is_punct("?") {
            return Err(self.unsupported("G037"));
        }
        if !self.is_punct("{") {
            return Ok(());
        }
        // `{` after an edge is a quantifier; property maps live inside brackets.
        self.bump();
        let lo = self.small_int()?;
        let hi = if self.eat_punct(",") {
            if self.is_punct("}") {
                return Err(self.unsupported("G061"));
            }
            self.small_int()?
        } else {
            lo
        };
        self.expect_punct("}")?;
        if hi < lo {
            return Err(ParseError::Syntax {
                at: self.at(),
                message: format!("quantifier upper bound {hi} is below lower bound {lo}"),
            });
        }
        e.hops = Some((lo, hi));
        Ok(())
    }

    fn small_int(&mut self) -> R<u32> {
        match self.bump() {
            Tok::Int(n) if (0..=u32::MAX as i128).contains(&n) => Ok(n as u32),
            other => {
                self.pos -= 1;
                Err(ParseError::Syntax {
                    at: self.at(),
                    message: format!("expected a non-negative integer, found {other}"),
                })
            }
        }
    }

    fn label_expr(&mut self) -> R<LabelExpr> {
        let mut left = self.label_and()?;
        while self.eat_punct("|") {
            left = LabelExpr::Or(Box::new(left), Box::new(self.label_and()?));
        }
        Ok(left)
    }

    fn label_and(&mut self) -> R<LabelExpr> {
        let mut left = self.label_atom()?;
        while self.eat_punct("&") {
            left = LabelExpr::And(Box::new(left), Box::new(self.label_atom()?));
        }
        Ok(left)
    }

    fn label_atom(&mut self) -> R<LabelExpr> {
        if self.eat_punct("!") {
            return Ok(LabelExpr::Not(Box::new(self.label_atom()?)));
        }
        if self.eat_punct("%") {
            return Ok(LabelExpr::Wildcard);
        }
        if self.eat_punct("(") {
            let e = self.label_expr()?;
            self.expect_punct(")")?;
            return Ok(e);
        }
        Ok(LabelExpr::Name(self.ident()?))
    }

    fn props(&mut self) -> R<Vec<(String, Expr)>> {
        self.expect_punct("{")?;
        let mut out: Vec<(String, Expr)> = Vec::new();
        if !self.is_punct("}") {
            loop {
                let at = self.at();
                let k = self.ident()?;
                if out.iter().any(|(e, _)| *e == k) {
                    return Err(ParseError::Syntax {
                        at,
                        message: format!("duplicate property name `{k}` (REQ-0011)"),
                    });
                }
                self.expect_punct(":")?;
                out.push((k, self.expr()?));
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        self.expect_punct("}")?;
        Ok(out)
    }

    // ------------------------------------------------------------------ expressions

    fn expr(&mut self) -> R<Expr> {
        self.binary(0)
    }

    fn binop(&self) -> Option<(BinOp, u8)> {
        let kw = |k: &str| self.is_kw(k);
        Some(match self.peek() {
            Tok::Ident(_) if kw("OR") => (BinOp::Or, 1),
            Tok::Ident(_) if kw("XOR") => (BinOp::Xor, 2),
            Tok::Ident(_) if kw("AND") => (BinOp::And, 3),
            Tok::Punct("=") => (BinOp::Eq, 5),
            Tok::Punct("<>") => (BinOp::Ne, 5),
            Tok::Punct("<") => (BinOp::Lt, 5),
            Tok::Punct("<=") => (BinOp::Le, 5),
            Tok::Punct(">") => (BinOp::Gt, 5),
            Tok::Punct(">=") => (BinOp::Ge, 5),
            Tok::Punct("||") => (BinOp::Concat, 6),
            Tok::Punct("+") => (BinOp::Add, 7),
            Tok::Punct("-") => (BinOp::Sub, 7),
            Tok::Punct("*") => (BinOp::Mul, 8),
            Tok::Punct("/") => (BinOp::Div, 8),
            Tok::Punct("%") => (BinOp::Mod, 8),
            _ => return None,
        })
    }

    fn binary(&mut self, min_prec: u8) -> R<Expr> {
        let mut left = self.unary(min_prec)?;
        loop {
            // Postfix predicates bind at comparison level.
            if min_prec <= 5 && self.at_postfix_predicate() {
                left = self.postfix_predicate(left)?;
                continue;
            }
            let Some((op, prec)) = self.binop() else {
                break;
            };
            if prec < min_prec {
                break;
            }
            self.bump();
            let right = self.binary(prec + 1)?;
            left = Expr::Binary(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn unary(&mut self, min_prec: u8) -> R<Expr> {
        if min_prec <= 4 && self.eat_kw("NOT") {
            return Ok(Expr::Unary(UnOp::Not, Box::new(self.binary(4)?)));
        }
        if self.eat_punct("-") {
            return Ok(Expr::Unary(UnOp::Neg, Box::new(self.unary(9)?)));
        }
        if self.eat_punct("+") {
            return self.unary(9);
        }
        let mut e = self.primary()?;
        while self.eat_punct(".") {
            e = Expr::Prop(Box::new(e), self.ident()?);
        }
        Ok(e)
    }

    fn at_postfix_predicate(&self) -> bool {
        self.is_kw("IS") || self.is_kw("IN") || (self.is_kw("NOT") && self.is_kw_at(1, "IN"))
    }

    /// `IS [NOT] NULL | LABELED | DIRECTED | SOURCE OF | DESTINATION OF | TYPED`, `[NOT] IN`.
    /// Called only when `at_postfix_predicate()` holds.
    fn postfix_predicate(&mut self, left: Expr) -> R<Expr> {
        if self.is_kw("NOT") && self.is_kw_at(1, "IN") {
            self.bump();
            self.bump();
            return Ok(Expr::In {
                expr: Box::new(left),
                list: Box::new(self.binary(6)?),
                negated: true,
            });
        }
        if self.eat_kw("IN") {
            return Ok(Expr::In {
                expr: Box::new(left),
                list: Box::new(self.binary(6)?),
                negated: false,
            });
        }
        self.expect_kw("IS")?;
        let negated = self.eat_kw("NOT");
        let b = Box::new(left);
        if self.eat_kw("NULL") {
            return Ok(Expr::IsNull { expr: b, negated });
        }
        if self.eat_kw("LABELED") {
            let label = if self.eat_punct(":")
                || matches!(self.peek(), Tok::Ident(_) | Tok::Quoted(_))
                    && !self.is_kw("AND")
                    && !self.is_kw("OR")
            {
                Some(self.label_expr()?)
            } else {
                None
            };
            return Ok(Expr::IsLabeled {
                expr: b,
                label,
                negated,
            });
        }
        if self.eat_kw("DIRECTED") {
            return Ok(Expr::IsDirected { expr: b, negated });
        }
        if self.is_kw("SOURCE") || self.is_kw("DESTINATION") {
            let source = self.eat_kw("SOURCE");
            if !source {
                self.bump();
            }
            self.expect_kw("OF")?;
            let edge = self.binary(6)?;
            return Ok(Expr::IsEndpoint {
                node: b,
                edge: Box::new(edge),
                source,
                negated,
            });
        }
        if self.eat_kw("TYPED") || self.is_punct("::") {
            self.eat_punct("::");
            let ty = self.type_name()?;
            return Ok(Expr::IsTyped {
                expr: b,
                ty,
                negated,
            });
        }
        Err(self.unexpected("NULL, LABELED, DIRECTED, SOURCE OF, DESTINATION OF or TYPED"))
    }

    fn primary(&mut self) -> R<Expr> {
        let at = self.at();
        match self.bump() {
            Tok::Int(n) => i64::try_from(n)
                .map(|v| Expr::Lit(Value::Int64(v)))
                .map_err(|_| ParseError::Syntax {
                    at,
                    message: format!("integer literal {n} is out of the int64 range"),
                }),
            Tok::Decimal(s) => {
                if s.contains(['e', 'E']) {
                    s.parse::<f64>()
                        .map(|v| Expr::Lit(Value::Double(v)))
                        .map_err(|e| ParseError::Syntax {
                            at,
                            message: e.to_string(),
                        })
                } else {
                    Decimal128::parse(&s)
                        .map(|d| Expr::Lit(Value::Decimal(d)))
                        .map_err(|e| ParseError::Syntax {
                            at,
                            message: e.to_string(),
                        })
                }
            }
            Tok::Suffixed(n, suffix) => {
                self.pos -= 1;
                let scientific = n.contains(['e', 'E']);
                Err(match suffix.to_ascii_lowercase().as_str() {
                    "m" if scientific => self.unsupported("GL06"),
                    "m" => self.unsupported("GL05"),
                    "f" => self.unsupported("GL09"),
                    "d" => self.unsupported("GL10"),
                    _ => ParseError::Syntax {
                        at,
                        message: format!("unknown numeric suffix `{suffix}`"),
                    },
                })
            }
            Tok::Str(s) => Ok(Expr::Lit(Value::String(s))),
            Tok::Param(p) => Ok(Expr::Param(p)),
            Tok::Punct("(") => {
                let e = self.expr()?;
                self.expect_punct(")")?;
                Ok(e)
            }
            Tok::Punct("[") => {
                let mut items = Vec::new();
                if !self.is_punct("]") {
                    loop {
                        items.push(self.expr()?);
                        if !self.eat_punct(",") {
                            break;
                        }
                    }
                }
                self.expect_punct("]")?;
                Ok(Expr::List(items))
            }
            Tok::Punct("{") => {
                self.pos -= 1;
                Ok(Expr::Record(self.props()?))
            }
            Tok::Quoted(s) => Ok(Expr::Var(s)),
            Tok::Ident(word) => self.word(word, at),
            other => {
                self.pos -= 1;
                Err(ParseError::Syntax {
                    at,
                    message: format!("expected an expression, found {other}"),
                })
            }
        }
    }

    /// An identifier in expression position: keyword literal, call, or variable.
    fn word(&mut self, word: String, at: usize) -> R<Expr> {
        let up = word.to_ascii_uppercase();
        match up.as_str() {
            "X" if matches!(self.peek(), Tok::Str(_)) => {
                let Tok::Str(hex) = self.bump() else {
                    unreachable!()
                };
                return parse_hex(&hex).map(|b| Expr::Lit(Value::Binary(b))).ok_or(
                    ParseError::Syntax {
                        at,
                        message: format!("invalid byte string literal X'{hex}'"),
                    },
                );
            }
            "TRUE" => return Ok(Expr::Lit(Value::Bool(true))),
            "FALSE" => return Ok(Expr::Lit(Value::Bool(false))),
            "NULL" => return Ok(Expr::Lit(Value::Null)),
            "RECORD" if self.is_punct("{") => return Ok(Expr::Record(self.props()?)),
            "LIST" | "ARRAY" if self.is_punct("[") => return self.primary(),
            "CASE" => return self.case(),
            "CAST" => {
                self.expect_punct("(")?;
                let expr = self.expr()?;
                self.expect_kw("AS")?;
                let ty = self.type_name()?;
                self.expect_punct(")")?;
                return Ok(Expr::Cast {
                    expr: Box::new(expr),
                    ty,
                });
            }
            "EXISTS" => {
                self.expect_punct("{")?;
                if self.is_kw("MATCH") {
                    self.bump();
                }
                let paths = self.path_list()?;
                if self.is_kw("MATCH") {
                    return Err(self.unsupported("GQ21"));
                }
                self.expect_punct("}")?;
                return Ok(Expr::Exists(paths));
            }
            "LET" => return Err(self.unsupported("GE03")),
            "VALUE" if self.is_punct("{") => return Err(self.unsupported("GQ18")),
            "PATH" if self.is_punct("[") => return Err(self.unsupported("GE06")),
            "DATE" | "TIME" | "LOCAL_DATETIME" | "LOCAL_TIME"
                if matches!(self.peek(), Tok::Str(_)) =>
            {
                self.pos -= 1;
                return Err(self.unsupported("GV39"));
            }
            "DURATION" if matches!(self.peek(), Tok::Str(_)) => {
                self.pos -= 1;
                return Err(self.unsupported("GV41"));
            }
            "INTERVAL" if matches!(self.peek(), Tok::Str(_)) => {
                self.pos -= 1;
                return Err(self.unsupported("GL12"));
            }
            "TIMESTAMP" | "ZONED_DATETIME" | "DATETIME" if matches!(self.peek(), Tok::Str(_)) => {
                let Tok::Str(s) = self.bump() else {
                    unreachable!()
                };
                return parse_timestamp(&s).map(|t| Expr::Lit(Value::Timestamp(t))).ok_or(ParseError::Syntax {
                    at,
                    message: format!("invalid zoned datetime literal '{s}' (expected e.g. 2026-10-07T12:00:00Z)"),
                });
            }
            _ => {}
        }
        if self.is_punct("(") {
            return self.call(up, at);
        }
        Ok(Expr::Var(word))
    }

    fn call(&mut self, name: String, at: usize) -> R<Expr> {
        self.expect_punct("(")?;
        if name == "TRIM" && !self.is_punct(")") {
            // Explicit TRIM (GF06): TRIM([LEADING | TRAILING | BOTH] [chars] FROM source).
            let save = self.pos;
            let mode = ["LEADING", "TRAILING", "BOTH"]
                .into_iter()
                .find(|m| self.eat_kw(m));
            let chars = if self.is_kw("FROM") {
                None
            } else {
                Some(self.expr()?)
            };
            if self.eat_kw("FROM") {
                let source = self.expr()?;
                self.expect_punct(")")?;
                let mut args = vec![
                    Expr::Lit(Value::String(mode.unwrap_or("BOTH").into())),
                    source,
                ];
                if let Some(c) = chars {
                    args.push(c);
                }
                return Ok(Expr::Call {
                    name: "TRIM".into(),
                    args,
                    distinct: false,
                });
            }
            if mode.is_some() {
                return Err(self.unexpected("FROM"));
            }
            self.pos = save;
        }
        if name == "COUNT" && self.eat_punct("*") {
            self.expect_punct(")")?;
            return Ok(Expr::CountStar);
        }
        match name.as_str() {
            "PATH_LENGTH" | "ELEMENTS" | "NODES" | "EDGES" => {
                self.pos -= 1;
                return Err(self.unsupported("GF04"));
            }
            "STDDEV_SAMP" | "STDDEV_POP" | "COLLECT" => {
                self.pos -= 1;
                return Err(self.unsupported("GF10"));
            }
            "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                self.pos -= 1;
                return Err(self.unsupported("GF11"));
            }
            _ => {}
        }
        if !KNOWN_FUNCTIONS.contains(&name.as_str()) {
            return Err(ParseError::Syntax {
                at,
                message: format!("unknown function {name}"),
            });
        }
        let distinct = self.eat_kw("DISTINCT");
        let mut args = Vec::new();
        if !self.is_punct(")") {
            loop {
                args.push(self.expr()?);
                if !self.eat_punct(",") {
                    break;
                }
            }
        }
        self.expect_punct(")")?;
        Ok(Expr::Call {
            name,
            args,
            distinct,
        })
    }

    fn case(&mut self) -> R<Expr> {
        let operand = if self.is_kw("WHEN") {
            None
        } else {
            Some(Box::new(self.expr()?))
        };
        let mut arms = Vec::new();
        while self.eat_kw("WHEN") {
            let w = self.expr()?;
            self.expect_kw("THEN")?;
            arms.push((w, self.expr()?));
        }
        if arms.is_empty() {
            return Err(self.unexpected("WHEN"));
        }
        let otherwise = if self.eat_kw("ELSE") {
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        self.expect_kw("END")?;
        Ok(Expr::Case {
            operand,
            arms,
            otherwise,
        })
    }

    fn type_name(&mut self) -> R<TypeName> {
        let word = self.ident()?.to_ascii_uppercase();
        let ty = match word.as_str() {
            "BOOL" | "BOOLEAN" => TypeName::Bool,
            "INT" | "INTEGER" | "INT64" | "INTEGER64" | "SIGNED" => {
                if word == "SIGNED" {
                    self.eat_kw("INTEGER");
                }
                TypeName::Int64
            }
            "FLOAT" | "DOUBLE" | "FLOAT64" => {
                if word == "DOUBLE" {
                    self.eat_kw("PRECISION");
                }
                if self.is_punct("(") {
                    return Err(self.unsupported("GV23"));
                }
                TypeName::Float64
            }
            "DECIMAL" | "DEC" => {
                if self.is_punct("(") {
                    return Err(self.unsupported("GV17"));
                }
                TypeName::Decimal
            }
            "STRING" | "VARCHAR" | "CHAR" => TypeName::String,
            "BYTES" | "BINARY" | "VARBINARY" => TypeName::Bytes,
            "ZONED" => {
                if self.eat_kw("TIME") {
                    return Err(self.unsupported("GV40"));
                }
                self.expect_kw("DATETIME")?;
                TypeName::ZonedDateTime
            }
            "TIMESTAMP" | "ZONED_DATETIME" => {
                if self.eat_kw("WITH") {
                    self.expect_kw("TIME")?;
                    self.expect_kw("ZONE")?;
                }
                TypeName::ZonedDateTime
            }
            "LIST" | "ARRAY" => TypeName::List,
            "RECORD" => TypeName::Record,
            other => {
                self.pos -= 1;
                let f = match other {
                    "UINT8" => "GV01",
                    "INT8" => "GV02",
                    "UINT16" => "GV03",
                    "INT16" => "GV04",
                    "USMALLINT" => "GV05",
                    "UINT32" => "GV06",
                    "INT32" => "GV07",
                    "UINT" => "GV08",
                    "UBIGINT" => "GV10",
                    "UINT64" => "GV11",
                    "UINT128" => "GV13",
                    "INT128" => "GV14",
                    "UINT256" => "GV15",
                    "INT256" => "GV16",
                    "SMALLINT" => "GV18",
                    "BIGINT" => "GV19",
                    "FLOAT16" => "GV20",
                    "FLOAT32" => "GV21",
                    "REAL" => "GV23",
                    "FLOAT128" => "GV25",
                    "FLOAT256" => "GV26",
                    "DATE" | "LOCAL" | "TIME" => "GV39",
                    "DURATION" => "GV41",
                    "PATH" => "GV55",
                    "GRAPH" => "GV60",
                    "TABLE" | "BINDING" => "GV61",
                    "ANY" => "GV65",
                    "NOTHING" => "GV72",
                    _ => {
                        return Err(ParseError::Syntax {
                            at: self.at(),
                            message: format!("unknown type {other}"),
                        });
                    }
                };
                return Err(self.unsupported(f));
            }
        };
        Ok(ty)
    }
}

/// Functions v1 knows (docs/gql-conformance.toml: GF01–GF03, GF05–GF07, GF12, GF13, G100,
/// plus the mandatory core and aggregates).
const KNOWN_FUNCTIONS: &[&str] = &[
    "ABS",
    "CEIL",
    "CEILING",
    "FLOOR",
    "SQRT",
    "EXP",
    "LN",
    "LOG",
    "LOG10",
    "POWER",
    "MOD",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "COT",
    "SINH",
    "COSH",
    "TANH",
    "DEGREES",
    "RADIANS",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "BTRIM",
    "UPPER",
    "LOWER",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "BYTE_LENGTH",
    "OCTET_LENGTH",
    "LEFT",
    "RIGHT",
    "SIZE",
    "CARDINALITY",
    "ELEMENT_ID",
    "COALESCE",
    "NULLIF",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "ALL_DIFFERENT",
    "SAME",
    "PROPERTY_EXISTS",
];

fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff](Z|±HH:MM)` -> microseconds UTC and offset.
fn parse_timestamp(s: &str) -> Option<Timestamp> {
    let (date, rest) = s.split_once(['T', ' '])?;
    let mut d = date.split('-');
    let (y, mo, dd): (i64, i64, i64) = (
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
    );
    let (time, offset_min) = if let Some(t) = rest.strip_suffix('Z') {
        (t, 0i16)
    } else {
        let idx = rest.rfind(['+', '-'])?;
        let (t, off) = rest.split_at(idx);
        let sign = if off.starts_with('-') { -1 } else { 1 };
        let (oh, om) = off[1..].split_once(':')?;
        (
            t,
            sign * (oh.parse::<i16>().ok()? * 60 + om.parse::<i16>().ok()?),
        )
    };
    let (hms, frac) = time.split_once('.').unwrap_or((time, "0"));
    let mut t = hms.split(':');
    let (h, mi, sec): (i64, i64, i64) = (
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&dd) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let micros_frac: i64 = format!("{frac:0<6}")[..6].parse().ok()?;
    // Days from civil (Howard Hinnant's algorithm).
    let (yy, m) = if mo <= 2 {
        (y - 1, mo + 9)
    } else {
        (y, mo - 3)
    };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let doy = (153 * m + 2) / 5 + dd - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let local = ((days * 24 + h) * 60 + mi) * 60 + sec;
    let utc = local - i64::from(offset_min) * 60;
    Timestamp::new(utc * 1_000_000 + micros_frac, offset_min).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_literals() {
        assert_eq!(
            parse_timestamp("1970-01-01T00:00:00Z").unwrap().micros_utc,
            0
        );
        let t = parse_timestamp("2026-10-07T12:30:00.5+05:30").unwrap();
        assert_eq!(t.offset_minutes, 330);
        assert_eq!(t.micros_utc, 1_791_356_400_500_000); // 07:00:00.5 UTC, checked with Python datetime
        assert!(parse_timestamp("2026-13-01T00:00:00Z").is_none());
    }
}
