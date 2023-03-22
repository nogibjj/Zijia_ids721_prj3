use actix_web::web::{Bytes, BytesMut};
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use csv::{ReaderBuilder, StringRecord};
use futures::stream::StreamExt;
use rusoto_core::{ByteStream, Region, RusotoError};
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use std::error::Error;

pub async fn get_csv_file() -> Result<Vec<u8>, std::io::Error> {
    let s3_client = rusoto_s3::S3Client::new(Region::default());
    let s3_obj = s3_client
        .get_object(GetObjectRequest {
            bucket: "ids721prj3".to_owned(),
            key: "WeatherEvents_Jan2016-Dec2021.csv".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut body = s3_obj.body.unwrap();
    let mut bytes = BytesMut::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk?);
    }

    let csv_file_content = bytes.to_vec();

    Ok(csv_file_content)
}

// given a string "weather_type", which is a column name in the csv file, return all records that have a value of "type" in that column
pub async fn get_matching_records(
    weather_type: String,
) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    let csv_file_content = get_csv_file().await?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(csv_file_content.as_slice());
    let mut matching_records = Vec::new();
    for result in rdr.records() {
        let record = result?;
        if record[1] == weather_type {
            matching_records.push(record);
            break;
        }
    }
    Ok(matching_records)
}

#[get("/{weather_type}")]
async fn get_report(path: web::Path<String>) -> impl Responder {
    let weather_type = path.into_inner();

    let matching_records = get_matching_records(weather_type).await.unwrap();
    let mut response = String::new();
    // put all the records into response
    for record in matching_records {
        response.push_str(&record[0]);
        response.push_str("\n");
    }

    HttpResponse::Ok().body(response)
}
