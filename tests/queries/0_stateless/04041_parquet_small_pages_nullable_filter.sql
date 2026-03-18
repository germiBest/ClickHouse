-- Tags: no-fasttest

insert into function file(currentDatabase() || '/04041.parquet')
    select number, if(number % 2, null, toString(number)) as s
    from numbers(100)
    settings output_format_parquet_max_dictionary_size=0, engine_file_truncate_on_insert=1, output_format_parquet_data_page_size=10, output_format_parquet_batch_size=1;

select * from file(currentDatabase() || '/04041.parquet') where number % 17 = 0 order by all settings input_format_parquet_max_block_size=10, input_format_parquet_use_native_reader_v3=1;
