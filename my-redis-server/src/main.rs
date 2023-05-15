use bytes::Bytes;
use mini_redis::{Frame};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
type Db = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;
use tokio::net::{TcpListener, TcpStream};

mod connection;
use connection::Connection;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening");
    let pool_num = 10;
    let mut db: Vec<Mutex<HashMap<String, Bytes>>> = Vec::with_capacity(10);
    for _ in 0..pool_num {
        db.push(Mutex::new(HashMap::new()))
    }

    let db: Db = Arc::new(db);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        // Clone the handle to the hash map.
        let db = db.clone();

        println!("Accepted");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};
    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    // dbg!(&connection);

    while let Some(frame) = connection.read_frame().await.expect("get frame error") {
        dbg!(&frame);
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let key = cmd.key();
                let key_hash = hash(key) as usize;
                let db = &db[key_hash % db.len()];

                let mut db = db.lock().unwrap();
                db.insert(key.to_string(), cmd.value().clone());
                dbg!(&db);
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let key = cmd.key();
                let key_hash = hash(key) as usize;
                let db = &db[key_hash % db.len()];

                let db = db.lock().unwrap();
                dbg!(&db);
                if let Some(value) = db.get(key) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}

fn hash(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize
}
