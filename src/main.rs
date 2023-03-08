// a web microservice that use actic web framework
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use futures::StreamExt;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectRequest, ListObjectsV2Error, ListObjectsV2Output, ListObjectsV2Request, S3Client, S3,
};
use std::env;

async fn index() -> impl Responder {
    HttpResponse::Ok().body("A weather report microservice. \r")
}

async fn list_bucket_objects(
    client: S3Client,
    bucket_name: &str,
) -> Result<ListObjectsV2Output, RusotoError<ListObjectsV2Error>> {
    let request = ListObjectsV2Request {
        bucket: bucket_name.to_string(),
        ..Default::default()
    };
    let result = client.list_objects_v2(request).await?;
    Ok(result)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env::set_var("AWS_ACCESS_KEY_ID", "AKIAZPVSLPTYRDERCKEN");
    env::set_var("AWS_SECRET_ACCESS_KEY", "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");

    let client = S3Client::new(Region::default());

    let result = list_bucket_objects(client, "ids721prj3").await;
    println!("{:#?}", result);

    HttpServer::new(|| App::new().route("/", web::get().to(index)))
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
