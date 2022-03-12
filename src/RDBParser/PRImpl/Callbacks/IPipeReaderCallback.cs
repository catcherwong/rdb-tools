using System.Buffers;

namespace RDBParser
{
    public interface IPipeReaderCallback
    {
        /// <summary>
        /// Called once we know we are dealing with a valid redis dump file
        /// </summary>
        /// <param name="version"></param>
        void StartRDB(int version);

        /// <summary>
        /// Called in the beginning of the RDB with various meta data fields such as:
        /// redis-ver, redis-bits, ctime, used-mem
        /// exists since redis 3.2 (RDB v7)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void AuxField(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value);

        /// <summary>
        /// Called to indicate database the start of database `db_number` 
        /// 
        /// Once a database starts, another database cannot start unless 
        /// the first one completes and then `end_database` method is called
        /// 
        /// Typically, callbacks store the current database number in a class variable
        /// </summary>
        /// <param name="database"></param>
        void StartDatabase(int database);


        /// <summary>
        /// Called to indicate start of a module key
        /// </summary>
        /// <param name="key">string. if key is None, this is module AUX data</param>
        /// <param name="module_name">string</param>
        /// <param name="expiry"></param>
        /// <param name="info">is a dictionary containing additional information about this object.</param>
        /// <returns></returns>
        bool StartModule(ReadOnlySequence<byte> key, string module_name, long expiry, Info info);

        void HandleModuleData(ReadOnlySequence<byte> key, ulong opCode, ReadOnlySequence<byte> data);

        void EndModule(ReadOnlySequence<byte> key, long bufferSize, ReadOnlySequence<byte> buffer);

        /// <summary>
        /// Called per database before the keys, with the key count in the main dictioney and the total voletaile key count
        /// exists since redis 3.2 (RDB v7)
        /// </summary>
        /// <param name="dbSize"></param>
        /// <param name="expiresSize"></param>
        void DbSize(uint dbSize, uint expiresSize);

        /// <summary>
        /// Callback to handle a key with a string value and an optional expiry
        /// </summary>
        /// <param name="key">the redis key</param>
        /// <param name="value">a string or a number</param>
        /// <param name="expiry">is a datetime object. None and can be None</param>
        /// <param name="info">a dictionary containing additional information about this object</param>
        void Set(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value, long expiry, Info info);

        /// <summary>
        /// Callback to handle the start of a hash
        /// 
        /// After `start_hash`, the method `hset` will be called with this `key` exactly `length` times.
        /// After that, the `end_hash` method will be called.
        /// </summary>
        /// <param name="key">the redis key</param>
        /// <param name="length">the number of elements in this hash. </param>
        /// <param name="expiry">a `datetime` object. None means the object does not expire</param>
        /// <param name="info">a dictionary containing additional information about this object.</param>
        void StartHash(ReadOnlySequence<byte> key, long length, long expiry, Info info);

        /// <summary>
        /// Callback to insert a field=value pair in an existing hash
        /// </summary>
        /// <param name="key">the redis key for this hash</param>
        /// <param name="field">a string</param>
        /// <param name="value">the value to store for this field</param>
        void HSet(ReadOnlySequence<byte> key, ReadOnlySequence<byte> field, ReadOnlySequence<byte> value);

        /// <summary>
        /// Called when there are no more elements in the hash
        /// </summary>
        /// <param name="key">the redis key for the hash</param>
        void EndHash(ReadOnlySequence<byte> key);

        /// <summary>
        /// Callback to handle the start of a set
        /// 
        /// After `start_set`, the  method `sadd` will be called with `key` exactly `cardinality` times
        /// After that, the `end_set` method will be called to indicate the end of the set.
        /// 
        /// Note : This callback handles both Int Sets and Regular Sets
        /// </summary>
        /// <param name="key">the redis key</param>
        /// <param name="cardinality">the number of elements in this set</param>
        /// <param name="expiry">a `datetime` object. None means the object does not expire</param>
        /// <param name="info">a dictionary containing additional information about this object.</param>
        void StartSet(ReadOnlySequence<byte> key, long cardinality, long expiry, Info info);

        /// <summary>
        /// Callback to inser a new member to this set
        /// </summary>
        /// <param name="key">the redis key for this set</param>
        /// <param name="member">the member to insert into this set</param>
        void SAdd(ReadOnlySequence<byte> key, ReadOnlySequence<byte> member);

        /// <summary>
        /// Called when there are no more elements in this set 
        /// </summary>
        /// <param name="key">the redis key for this set</param>
        void EndSet(ReadOnlySequence<byte> key);

        /// <summary>
        /// Callback to handle the start of a list
        /// 
        /// After `start_list`, the method `rpush` will be called with `key` exactly `length` times
        /// After that, the `end_list` method will be called to indicate the end of the list
        /// 
        /// Note : This callback handles both Zip Lists and Linked Lists.
        /// </summary>
        /// <param name="key">the redis key for this list</param>
        /// <param name="expiry">a `datetime` object. None means the object does not expire</param>
        /// <param name="info">a dictionary containing additional information about this object.</param>
        void StartList(ReadOnlySequence<byte> key, long expiry, Info info);

        /// <summary>
        /// Callback to insert a new value into this list
        /// 
        /// Elements must be inserted to the end (i.e. tail) of the existing list.
        /// </summary>
        /// <param name="key">the redis key for this list</param>
        /// <param name="value">the value to be inserted</param>
        void RPush(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value);

        /// <summary>
        /// Called when there are no more elements in this list
        /// </summary>
        /// <param name="key">the redis key for this list</param>
        /// <param name="info">a dictionary containing additional information about this object that wasn't known in start_list.</param>
        void EndList(ReadOnlySequence<byte> key, Info info);

        /// <summary>
        /// Callback to handle the start of a sorted set
        /// </summary>
        /// <param name="key">the redis key for this sorted</param>
        /// <param name="length">the number of elements in this sorted set</param>
        /// <param name="expiry">a `datetime` object. None means the object does not expire</param>
        /// <param name="info">a dictionary containing additional information about this object.</param>
        void StartSortedSet(ReadOnlySequence<byte> key, long length, long expiry, Info info);

        /// <summary>
        /// Callback to insert a new value into this sorted set
        /// </summary>
        /// <param name="key">the redis key for this sorted set</param>
        /// <param name="score">the score for this `value`</param>
        /// <param name="member">the element being inserted</param>
        void ZAdd(ReadOnlySequence<byte> key, double score, ReadOnlySequence<byte> member);

        /// <summary>
        /// Called when there are no more elements in this sorted set
        /// </summary>
        /// <param name="key">the redis key for this sorted set</param>
        void EndSortedSet(ReadOnlySequence<byte> key);

        /// <summary>
        /// Callback to handle the start of a stream
        /// 
        /// After `start_stream`, the method `stream_listpack` will be called with this `key` exactly `listpacks_count` times.
        /// After that, the `end_stream` method will be called.
        /// </summary>
        /// <param name="key">the redis key</param>
        /// <param name="listpacks_count">the number of listpacks in this stream.</param>
        /// <param name="expiry">a `datetime` object. None means the object does not expire</param>
        /// <param name="info">a dictionary containing additional information about this object</param>
        void StartStream(ReadOnlySequence<byte> key, long listpacks_count, long expiry, Info info);

        /// <summary>
        /// Callback to insert a listpack into a stream
        /// </summary>
        /// <param name="key">the redis key for this stream</param>
        /// <param name="entry_id">binary (bigendian)</param>
        /// <param name="data">the bytes of the listpack</param>
        void StreamListPack(ReadOnlySequence<byte> key, ReadOnlySequence<byte> entry_id, ReadOnlySequence<byte> data);

        /// <summary>
        /// Called when there is no more data in the stream
        /// </summary>
        /// <param name="key">redis key for the stream</param>
        /// <param name="items">total number of items in the stream</param>
        /// <param name="last_entry_id">in "<millisecondsTime>-<sequenceNumber>" format</param>
        /// <param name="cgroups">an array of consumer group metadata</param>
        void EndStream(ReadOnlySequence<byte> key, ulong items, string last_entry_id, StreamGroup cgroups);

        /// <summary>
        /// Called when the current database ends
        /// 
        ///  After `end_database`, one of the methods are called - 
        ///  1) `start_database` with a new database number
        ///     OR
        ///  2) `end_rdb` to indicate we have reached the end of the file   
        /// </summary>
        /// <param name="dbNumber"></param>
        void EndDatabase(int dbNumber);

        /// <summary>
        /// Called to indicate we have completed parsing of the dump file
        /// </summary>
        void EndRDB();
    }
}
