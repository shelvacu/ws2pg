#![recursion_limit="1024"] // See https://github.com/rust-lang/futures-rs/issues/1917
use std::error::Error;
use std::collections::HashMap;
use std::borrow::Cow;
use std::fmt::{self, Display};

use futures::stream::{self, StreamExt, TryStreamExt};
use futures::{Sink, SinkExt};
use futures::lock::Mutex;

use tokio::net::{TcpListener, TcpStream};

use tokio_tungstenite::tungstenite::Message;

use tokio_postgres::{NoTls, Statement};
use tokio_postgres::types::{Type, FromSql, ToSql};

use serde_derive::{Serialize, Deserialize};

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
struct IncorrectParamLength;

impl Display for IncorrectParamLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error: Incorrect param length")
    }
}

impl Error for IncorrectParamLength {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ty", content = "val")]
#[serde(rename_all = "lowercase")]
enum SqlType {
    #[serde(alias = "boolean")]
    Bool(bool),
    #[serde(alias = "int1")]
    Char(i8),
    #[serde(alias = "int2")]
    #[serde(alias = "smallserial")]
    SmallInt(i16),
    #[serde(alias = "serial")]
    #[serde(alias = "int4")]
    Int(i32),
    Oid(u32),
    #[serde(alias = "int8")]
    #[serde(alias = "bigserial")]
    BigInt(i64),
    #[serde(alias = "float4")]
    Real(f32),
    #[serde(alias = "float8")]
    #[serde(alias = "double precision")]
    Double(f64),
    #[serde(alias = "string")]
    #[serde(alias = "varchar")]
    Text(String),
    Bytea(Vec<u8>),
    Json(serde_json::Value),
    Jsonb(serde_json::Value),
}

impl SqlType {
    fn as_dyn(&self) -> &(dyn ToSql + Sync) {
        macro_rules! inner {
            ( $( $t:ident, )* ) => {
                match *self {
                    $( Self::$t(ref inner) => return inner as &(dyn ToSql + Sync), )*
                }
            };
        }
        inner!{
            Bool,
            Char,
            SmallInt,
            Int,
            Oid,
            BigInt,
            Real,
            Double,
            Text,
            Bytea,
            Json,
            Jsonb,
        }
    }
}

impl FromSql<'_> for SqlType {
    fn accepts(ty: &Type) -> bool {
        macro_rules! accepted_types {
            ( $( $t:ident ),* ) => {
                false $( || ty == &Type::$t )*
            };
        }
        //return ty == &Type::BOOL || ty == &Type::INT8;
        return accepted_types!(BOOL, CHAR, INT2, INT4, OID, INT8, FLOAT4, FLOAT8, TEXT, BYTEA, JSON, JSONB); 
    }

    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + 'static + Sync + Send>> {
        macro_rules! convert_types {
            ( $( $c:ident $e:ident $t:ty, )* ) => {
                if false {
                    unreachable!();
                } $( else if ty == &Type::$c {
                    return Ok(Self::$e(<$t as FromSql>::from_sql(ty, raw)?));
                } )* else {
                    unreachable!();
                }
            }
        }
        convert_types! {
            BOOL Bool bool,
            CHAR Char i8,
            INT2 SmallInt i16,
            INT4 Int i32,
            OID Oid u32,
            INT8 BigInt i64,
            FLOAT4 Real f32,
            FLOAT8 Double f64,
            TEXT Text String,
            BYTEA Bytea Vec<u8>,
            JSON Json serde_json::Value,
            JSONB Jsonb serde_json::Value,
        }
        // if ty == &Type::BOOL {
        //     return Ok(Self::Bool(<bool as FromSql>::from_sql(ty, raw)?));
        // } else if ty == &Type::CHAR {
        //     return Ok(Self::Char(<i8 as FromSql>::from_sql(ty, raw)?));
        // } else {
        //     unreachable!();
        // }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum StringOrIntStatement {
    Str(String),
    Id(u32),
}

impl StringOrIntStatement {
    async fn into_statement<'a, 'b>(self, prepareds: &'a HashMap<u32, Statement>, client: &'b tokio_postgres::Client) -> Result<Cow<'a, Statement>, tokio_postgres::Error> {
        match self {
            Self::Str(s) => client.prepare(&s).await.map(|s| Cow::Owned(s)),
            Self::Id(id) => Ok(Cow::Borrowed(&prepareds[&id])),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "ty")]
#[serde(rename_all = "lowercase")]
enum Ws2PgMessage {
    Query{statement: StringOrIntStatement, params: Vec<SqlType>, msgid: u32},
    Prepare{statement: String, msgid: u32},
}

impl Ws2PgMessage {
    fn msgid(&self) -> u32 {
        match self {
            Self::Query{msgid, ..} => *msgid,
            Self::Prepare{msgid, ..} => *msgid,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "ty")]
#[serde(rename_all = "lowercase")]
enum Pg2WsMessage {
    Results{rows: Vec<Vec<SqlType>>, msgid: u32},
    Prepared{id: u32, msgid: u32},
    Error{msg: String, msgid: Option<u32>},
    Notification{process_id: i32, channel: String, payload: String},
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv()?;
    let args:Vec<_> = std::env::args().collect();
    let bind_addr;
    if args.len() < 2 {
        bind_addr = std::env::var("LISTEN_ADDR")?;
    } else {
        bind_addr = args[1].clone();
    }
    let mut socket = TcpListener::bind(&bind_addr).await?;

    let connection_str = std::env::var("PG_CONNECTION")?;
    {
        let (_,_) = tokio_postgres::connect(&connection_str, NoTls).await?;
    }

    while let Ok((stream, _)) = socket.accept().await {
        stream.set_nodelay(true).unwrap();
        tokio::spawn(accept_connection(stream, connection_str.clone()));
    }

    return Ok(())
}

fn encode(stct: Pg2WsMessage) -> futures::future::Ready<Result<Message, anyhow::Error>> {
    #[cfg(feature = "dbg")]
    dbg!(&stct);
    futures::future::ready(Ok(Message::binary(
        rmp_serde::encode::to_vec_named(
            &stct
        ).unwrap()
    )))
}

async fn accept_connection(stream: TcpStream, connection_str: String) {
    //let addr = stream.peer_addr().unwrap();
    let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
    let (pg_client, mut pg_connection) = tokio_postgres::connect(&connection_str, NoTls).await.unwrap();
    let (ws_sender, mut ws_receiver) = ws_stream.split();
    let ws_sender_mut = Mutex::new(ws_sender.with(encode));
    let mut pg_stream = stream::poll_fn(move |cx| pg_connection.poll_message(cx)).map_err(|e| panic!(e));
    futures::join!{
        async { 
            while let Some(res) = pg_stream.next().await {
                let msg = res.unwrap();
                match msg {
                    tokio_postgres::AsyncMessage::Notice(notice) => { 
                        #[cfg(feature = "dbg")]
                        dbg!(notice);
                    },
                    tokio_postgres::AsyncMessage::Notification(notif) => {
                        let response = Pg2WsMessage::Notification{process_id: notif.process_id(), channel: notif.channel().into(), payload: notif.payload().into()};
                        ws_sender_mut.lock().await.send(
                            response
                        ).await.unwrap_or_else(|_| panic!("could not send"))
                    },
                    _ => (),
                }
            }
        },
        async {
            let mut prepared_statements:HashMap<u32, Statement> = HashMap::new();
            let mut prepared_statement_ticker = 0;
            while let Some(res) = ws_receiver.next().await {
                #[cfg(feature = "dbg")]
                println!("{:?}", &res);
                let res = res.unwrap();
                if res.is_text() {
                    let mut sender_lock = ws_sender_mut.lock().await;
                    sender_lock.send(
                        Pg2WsMessage::Error{msg: "Must send binary frames, no text frames allowed".to_string(), msgid: None}
                    ).await.unwrap();
                    sender_lock.close().await.unwrap();
                } else if res.is_binary() {
                    //do stuff
                    let mut c = std::io::Cursor::new(res.into_data());
                    let parsed_res:Result<Ws2PgMessage,_> = rmp_serde::decode::from_read(&mut c);
                    let parsed = match parsed_res {
                        Err(e) => {
                            let mut sender_lock = ws_sender_mut.lock().await;
                            sender_lock.send(
                                Pg2WsMessage::Error{msg: format!("Invalid syntax or structure: {}", &e), msgid: None}
                            ).await.unwrap();
                            sender_lock.close().await.unwrap();
                            panic!();
                        },
                        Ok(parsed) => parsed,
                    };
                    #[cfg(feature = "dbg")]
                    dbg!(&parsed);
                    let msgid = parsed.msgid();
                    match handle_msg(parsed, &ws_sender_mut, &pg_client, &mut prepared_statements, &mut prepared_statement_ticker).await {
                        Err(e) => send_err(&ws_sender_mut, e, msgid).await,
                        Ok(_) => (),
                    }
                } //ignore others
            }
        }
    };
}

async fn handle_msg(
    msg: Ws2PgMessage,
    ws_sender_mut: &Mutex<impl Sink<Pg2WsMessage> + std::marker::Unpin>,
    pg_client: &tokio_postgres::Client,
    prepared_statements: &mut HashMap<u32, Statement>,
    prepared_statement_ticker: &mut u32,
) -> Result<(), anyhow::Error> {
    match msg {
        Ws2PgMessage::Query{statement: statement_any, params, msgid} => {
            let params_sql:Vec<_> = params.iter().map(|p| p.as_dyn()).collect();
            let statement = statement_any.into_statement(prepared_statements, pg_client).await?;
            if statement.params().len() != params.len() {
                Err(IncorrectParamLength)?;
            }
            let result = pg_client.query(statement.as_ref(), &params_sql[..]).await?;
            let rows:Vec<_> = result.iter().map(|pg_row| {
                let mut row:Vec<SqlType> = Vec::new();
                for i in 0..pg_row.len() {
                    row.push(pg_row.get(i));
                }
                row
            }).collect();
            let response = Pg2WsMessage::Results{rows, msgid};
            ws_sender_mut.lock().await.send(
                response
            ).await.unwrap_or_else(|_| panic!("could not send"))
        },
        Ws2PgMessage::Prepare{statement, msgid} => {
            let id:u32 = *prepared_statement_ticker;
            *prepared_statement_ticker += 1;
            let result = pg_client.prepare(statement.as_str()).await?;
            prepared_statements.insert(id, result);
            ws_sender_mut.lock().await.send(
                Pg2WsMessage::Prepared{id, msgid}
            ).await.unwrap_or_else(|_| panic!("could not send"))
        },
    }

    Ok(())
}

async fn send_err(ws_sender_mut: &Mutex<impl Sink<Pg2WsMessage> + std::marker::Unpin>, e: anyhow::Error, msgid: u32) {
    ws_sender_mut.lock().await.send(
        Pg2WsMessage::Error{
            msg: format!("{}", e),
            msgid: Some(msgid),
        }
    ).await.unwrap_or_else(|_| panic!("could not send"));
}

