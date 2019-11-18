use reqwest;
use reqwest::Client;
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use cs_cityio_backend::{connect, send_table};
use regex::Regex;
use std::{thread, time};
use std::env;
use dotenv::dotenv;
use std::collections::HashMap;


const BASE_URL: &str = "https://cityio.media.mit.edu/api";

fn main() {

    dotenv().ok();

    let mut hashmap: HashMap<String, String> = HashMap::new();

    let cityio_module_key = env::var("CITYIO_MODULE_KEY").unwrap();

    let list_end_point = format!("{}/tables/list", BASE_URL);

    let interval = time::Duration::from_secs(60 * 60); // 1H

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
            let table_data: Value = client.get(&url)
                .header(AUTHORIZATION, token)
                .send()
                .unwrap_or_else(|_| panic!("Error getting table {}", &url))
                .json()
                .expect("Could not parse table to json");

            // gets the last word
            let re = Regex::new(r"(\w*).$").unwrap();

            let table_name = re.captures(&url).map(|c| {
                let tn = c.get(0).map(|m| m.as_str());
                tn.unwrap()
            }).unwrap();

            println!("table name: {}", &table_name);

            let id = get_id(&table_data).unwrap();

            if !hashmap.contains_key(table_name) {
                hashmap.insert(table_name.to_string(), id.to_string());
            } else {
                let prev_id = hashmap.get(table_name).unwrap();
                if prev_id == id {
                    println!("no change since last write");    
                    println!();
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

    meta.get("id").map(|id| id.as_str().expect("Invalid String"))
}
