use std::path::Path;

use arrow::array::{BinaryArray, Int64Array};
use arrow::datatypes::DataType;
use eyre::ContextCompat;
use futures::StreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek};

use crate::item_kind::ItemKindMarker;

pub async fn open_dir_files(dir: impl AsRef<Path>) -> eyre::Result<Vec<File>> {
    let mut read_dir = tokio::fs::read_dir(dir).await?;

    let mut file_paths = vec![];

    while let Ok(Some(entry)) = read_dir.next_entry().await {
        if entry.metadata().await?.is_file() {
            file_paths.push(entry.path());
        }
    }

    file_paths.sort();

    let mut files = vec![];
    for file_path in file_paths {
        let file = File::open(file_path).await?;
        files.push(file);
    }

    Ok(files)
}

pub async fn read_parquet<I, T>(file: T) -> eyre::Result<Vec<(i64, I::Type)>>
where
    // Generic over any type that is async readable
    T: AsyncRead + AsyncSeek + Unpin + Send + 'static,
    I: ItemKindMarker,
    // The type is safe to send across threads and owns its own data
    <I as ItemKindMarker>::Type: Send + Sync + 'static,
    // The the type can be created from a slice of bytes
    for<'a> <I as ItemKindMarker>::Type: TryFrom<&'a [u8]>,
    // The conversion error is a standard error
    for<'a> <<I as ItemKindMarker>::Type as TryFrom<&'a [u8]>>::Error:
        std::error::Error + Send + Sync + 'static,
{
    let mut parquet_reader =
        ParquetRecordBatchStreamBuilder::new(file).await?.build()?;

    let schema = parquet_reader.schema().clone();
    let (id_idx, id_field) = schema
        .column_with_name("id")
        .context("Missing column `id`")?;
    let (item_idx, item_field) = schema
        .column_with_name(I::PARQUET_COLUMN_NAME)
        .context("Missing item column")?;

    eyre::ensure!(id_field.data_type() == &DataType::Int64);
    eyre::ensure!(item_field.data_type() == &DataType::Binary);

    let mut items = vec![];

    while let Some(batch) = parquet_reader.next().await {
        let batch = batch?;

        let id_array = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Failed to cast to Int64Array")?;

        let item_array = batch
            .column(item_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("Failed to cast to BinaryArray")?;

        for i in 0..batch.num_rows() {
            let id = id_array.value(i);
            let item = item_array.value(i);

            let item = I::Type::try_from(item)?;
            items.push((id, item));
        }
    }

    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item_kind::Shares;

    #[tokio::test]
    async fn validate_share_parquet_files() -> eyre::Result<()> {
        let files = open_dir_files("src/snapshot").await?;

        for file in files {
            read_parquet::<Shares, _>(file)
                .await
                .expect("Failed to read shares");
        }

        Ok(())
    }
}
