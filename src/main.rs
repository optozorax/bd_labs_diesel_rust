#[macro_use]
extern crate diesel;

use diesel::prelude::*;
use diesel::sql_query;

fn task1(connection: &PgConnection) {
    table! {
        task1 (n_det) {
            n_det -> Text,
            min -> Integer,
        }
    }

    #[derive(QueryableByName, Debug)]
    #[table_name = "task1"]
    struct Task1 {
        n_det: String,
        min: i32,
    }

    let task1: Vec<Task1> = sql_query("
        select n_det, min(kol) 
        from spj 
        where n_det in (
            select n_det 
            from p
        ) group by n_det")
        .get_results(connection)
        .expect("Task1 error");

    println!("{} results:", task1.len());
    for post in task1 {
        println!("    {:?}", post);
    }
}

fn task2(connection: &PgConnection) {
    table! {
        task2 (n_det) {
            n_det -> Text,
            cvet -> Text,
            ves -> Integer,
            sum_weight -> BigInt,
        }
    }

    #[derive(QueryableByName, Debug)]
    #[table_name = "task2"]
    struct Task2 {
        n_det: String,
        cvet: String,
        ves: i32,
        sum_weight: i64,
    }

    let task2: Vec<Task2> = sql_query("
        select t.n_det, p.cvet, p.ves, t.sum_weight
        from p
        join (
            select n_det, sum(spj.kol) as sum_weight 
            from spj 
            where n_izd = ? 
            group by spj.n_det 
        ) t on p.n_det = t.n_det"
        .replacen("?", "\'J1\'", 1))
        .get_results(connection)
        .expect("Task2 error");

    println!("{} results:", task2.len());
    for post in task2 {
        println!("    {:?}", post);
    }
}

fn main() {
    let database_url = "\
        postgres://\
            pmi-bXXXX\
            :PASSWORD\
            @fpm2.ami.nstu.ru\
            /students\
            ?options=-c search_path%3D\
            pmibXXXX";

    let connection = PgConnection::establish(&database_url)
        .expect("Error connecting to database");

    task1(&connection);
    task2(&connection);
}
