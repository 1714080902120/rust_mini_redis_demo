use bytes::Bytes;
use std::{io};
use tokio::sync::{mpsc::{self, Sender}, oneshot};

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;


#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

#[tokio::main]
async fn main() {
    loop {
        println!("follow rules: [cmd] [key] [value?]");
        let mut user_res = String::from("");
        io::stdin()
            .read_line(&mut user_res)
            .expect("unexpect error");
        let mut v: Vec<String> = user_res.split_whitespace().map(|s| s.to_string()).collect();
        let mut cmd = v[0].clone();
        if v.len() < 2 {
            println!("your input seem to be missing parameters, check it out");
            continue;
        }
        let key = v[1].clone();

        let (tx, mut rx) = mpsc::channel(32);
        let manager = tokio::spawn(async move {
            use mini_redis::client;
            let mut client = client::connect("127.0.0.1:6379").await.unwrap();
    
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Command::Get { key, resp } => {
                        let res = client.get(key.as_str()).await;
                        resp.send(res).unwrap();
                    }
                    Command::Set { key, val, resp } => {
                        let res = client.set(key.as_str(), val).await;
                        resp.send(res).unwrap();
                    }
                    cmd => panic!("the command {:?} is not support!", cmd),
                }
            }
        });

        let task = tokio::spawn(async move {
            match cmd.as_str() {
                "get" => {
                    spawn_task_get(key, tx).await;
                }
                "set" => {
                    let value = v[2].clone();
                    spawn_task_set(key, value.into(), tx).await;
                }
                t => {
                    println!("command {t} is not support!");
                }
            };
        });
        task.await.unwrap();
        manager.await.unwrap();
    }
}

async fn spawn_task_set(key: String, val: Bytes, tx: Sender<Command>) {
    let (sd, rc) = oneshot::channel();
    let data = Command::Set { key, val, resp: sd };
    tx.send(data).await.unwrap();

    rc.await;
    dbg!("set success");
}

async fn spawn_task_get(key: String, tx: Sender<Command>) {
    let (sd, rc) = oneshot::channel();
    let data = Command::Get { key, resp: sd };
    tx.send(data).await.unwrap();

    let res = rc.await;
    dbg!(res);
}
