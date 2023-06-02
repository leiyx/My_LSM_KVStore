
file(GLOB UTIL_SRC util/**.cc)

set(DB_SRC
"db/dbimpl.cc"
"db/option.cc"
"db/filter/filter_block.cc"
"db/filter/bloom.cc"
"db/format/internal_key.cc"
"db/log/log_reader.cc"
"db/log/log_writer.cc"
"db/memtable/arena.cc"
"db/memtable/memtable.cc"
"db/sstable/block_builder.cc"
"db/sstable/block_format.cc"
"db/sstable/block_reader.cc"
"db/sstable/sstable_builder.cc"
"db/sstable/sstable_reader.cc"
"db/sstable/table_cache.cc"
"db/version/merge.cc"
"db/version/version_edit.cc"
"db/version/version.cc"
"db/writebatch/writebatch.cc"
)


set(SRCS 
	${UTIL_SRC}
	${DB_SRC}
	)

add_library(lsmkv ${SRCS})

target_link_libraries(lsmkv snappy crc32c pthread)