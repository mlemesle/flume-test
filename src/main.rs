use std::time::Duration;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    nb_workers: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();

    let mut handles = Vec::with_capacity(args.nb_workers as usize);

    let (sendr, recvr) = flume::bounded(args.nb_workers as usize);

    tracing::info!("Starting workers");

    for i in 0..args.nb_workers {
        let own_recrv = recvr.clone();
        handles.push(tokio::spawn(async move {
            tracing::info!("Worker {i} starts consuming");
            while let Ok(id) = own_recrv.recv_async().await {
                tokio::time::sleep(Duration::from_secs(i % 3)).await;
                tracing::info!("Task {i} got message id {id} !")
            }
            tracing::info!("Worker {i} stops consuming");
        }));
    }

    let max = 100_000;
    tracing::info!("Sending {max} ids");
    for j in 0..max {
        sendr.send_async(j).await?;
    }
    tracing::info!("{max} ids sent");

    drop(sendr);
    tracing::info!("Join all");
    futures::future::join_all(handles).await;

    tracing::info!("End");

    Ok(())
}
