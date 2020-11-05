use reqwest;
use reqwest::Client;
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use cs_cityio_backend::{connect, send_table};
use regex::Regex;
use std::{thread, time};
use std::env;
use std::fs::File;
use std::path::Path;
use dotenv::dotenv;
use std::collections::HashMap;
use serde::Deserialize;
use toml;
use std::io::prelude::*;
use std::io::Error;
use std::io::ErrorKind::InvalidData;


const BASE_URL: &str = "https://cityio.media.mit.edu/api";
// const BASE_URL: &str = "http://localhost:8080/api";

#[derive(Debug, Deserialize)]
struct Config {
    interval: u64,
}

impl Config {
    fn new (interval: u64) -> Self {
        Self {
            interval
        }
    }
}

fn main() {

    dotenv().ok();

    // read config
    let path = Path::new("config.toml");
    let config = match File::open(path)
        .and_then(|mut file| {
            // save it to buffer
            let mut contents: Vec<u8> = Vec::new();
            file.read_to_end(&mut contents).unwrap();
            Ok(contents)
        })
        .and_then(|c| {
            // parse the contents to Config through toml
            toml::from_slice::<Config>(&c).map_err(|_| Error::new(InvalidData, "invalid toml file"))
        })
        {
            Ok(config) => config,
            Err(_) => Config::new(3600)
        };

    let mut hashmap: HashMap<String, String> = HashMap::new();

    let cityio_module_key = env::var("CITYIO_MODULE_KEY").unwrap();

    let list_end_point = format!("{}/tables/list", BASE_URL);

    let interval = time::Duration::from_secs(config.interval); // 1H
    println!("saving every {} seconds.", &interval.as_secs());

    loop{
        println!("************************");
        println!("backup start");
        println!("************************");


        let resp: Vec<String> = reqwest::get(&list_end_point)
            .expect("Error getting table list")
            .json()
            .expect("Could not parse table list json to data");

        for url in resp {

            let token = format!("Bearer {}", &cityio_module_key);

            let client = Client::new();
            let table_data: Value = match client.get(&url)
                .header(AUTHORIZATION, token)
                .send()
                .and_then(|mut table| table.json())
                {
                    Ok(table) => table,
                    Err(_) => continue
                };

            // gets the last word
            let re = Regex::new(r"(\w*).$").unwrap();

            // let table_name = re.captures(&url).map(|h|{h.get(0)}).map(|m| m.to_str());

            let table_name = match re.captures(&url).and_then(|cap| cap.get(0)).map(|m| m.as_str()) {
                Some(tn) => tn,
                None => continue
            };

            println!("table name: {}", &table_name);

            let id = match get_id(&table_data) {
                Some(id) => id,
                None => {
                    println!("weird... could not find the id for this table");
                    continue
                },
            };

            if !hashmap.contains_key(table_name) {
                hashmap.insert(table_name.to_string(), id.to_string());
            } else {
                let prev_id = hashmap.get(table_name).unwrap();
                if prev_id == id {
                    println!("no change since last write");    
                    continue;
                }
            }

            let con = connect();
            match send_table(&con, id, &table_name, &table_data) {
                Ok(()) => (),
                Err(e) => println!("{}", e),
            };
            println!();
        }
        println!();
        println!();
        thread::sleep(interval);
    }
}

fn get_id(table: &Value) -> Option<&str> {

    let meta = match table.get("meta") {
        Some(m) => m,
        None => return None
    };

    meta.get("id").map(|id| id.as_str()).flatten()
}
