use dotenv;

use std::convert::Infallible;
use std::env;
use std::time::Duration;

use warp::http::StatusCode;
use warp::{reject, Filter, Rejection, Reply};

use serde;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPool, PgPoolOptions};

use rustflake::Snowflake;

#[derive(Deserialize, Serialize, Debug)]
struct Gifs {
    gif: Vec<Gif>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Gif {
    id: i64,
    url: String,
    category: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct UrlQuery {
    url: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct Id {
    id: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let pool = get_pool().await?;
    let with_db = warp::any().map(move || pool.clone());

    // Match `/:Seconds`...
    let wait = warp::path::param()
        // and_then create a `Future` that will simply wait N seconds...
        .and_then(|seconds| sleepy(seconds));

    let stringy = warp::path!("re" / String).map(|string: String| string.replace("%20", " "));

    let random_gif = warp::path!("api" / "gif" / String)
        .and(with_db.clone())
        .and_then(|cat, postgres: PgPool| get_gifs(cat, postgres));

    let add_gif = warp::path!("api" / "gif" / String)
        .and(warp::query::<UrlQuery>())
        .and(with_db.clone())
        .and_then(|cat, url: UrlQuery, postgres: PgPool| post_gifs(url.url, cat, postgres));

    let routes = warp::get()
        .and(wait.or(stringy).or(add_gif).or(random_gif))
        .recover(handle_rejection);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
    Ok(())
}

async fn sleepy(seconds: u8) -> Result<impl warp::Reply, Infallible> {
    tokio::time::delay_for(Duration::from_secs(seconds.into())).await;
    Ok(format!("I waited {} seconds!", seconds))
}

pub async fn get_pool() -> anyhow::Result<PgPool, anyhow::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&env::var("DATABASE_URL")?)
        .await?;
    println!(
        "Connected to the database at url {}",
        &env::var("DATABASE_URL")?
    );
    Ok(pool)
}

async fn get_gifs(cat: String, pool: PgPool) -> Result<impl Reply, Rejection> {
    let gifs = sqlx::query_as!(
        Gif,
        "
        select id, url, category
        from gif_gifs 
        WHERE category = $1
        order by random() 
        limit 1
        ",
        cat
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    if gifs.len() == 0 {
        return Err(reject::not_found());
    }

    Ok(warp::reply::json(&gifs[0]))
}

async fn post_gifs(url: String, cat: String, pool: PgPool) -> Result<impl Reply, Rejection> {
    let gifs = sqlx::query_as!(
        Id,
        "
        INSERT INTO public.gif_gifs (id, url, category) 
        VALUES ($1, $2, $3)
        returning id;
        ",
        gen_flake().await,
        url,
        cat
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    Ok(warp::reply::json(&gifs))
}

async fn gen_flake() -> i64 {
    // Using Discord epoch
    let mut snowflake = Snowflake::new(1420070400000, 1, 1);
    return snowflake.generate();
}

/// An API error serializable to JSON.
#[derive(Serialize)]
struct ErrorMessage {
    code: u16,
    message: String,
}
// This function receives a `Rejection` and tries to return a custom
// value, otherwise simply passes the rejection along.
async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND";
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        // We can handle a specific error, here METHOD_NOT_ALLOWED,
        // and render it however we want
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED";
    } else {
        // We should have expected this... Just log and say its a 500
        eprintln!("unhandled rejection: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION";
    }

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}
