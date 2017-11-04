# ORC Dynamic Partitioned Dataset Batch Sink


Description
-----------
Sink for a ``PartitionedFileSet`` that writes data in ORC format
and leverages one or more record field values for creating partitions.
All data for the run will be written to a partition based on the
specified fields and their value.


Use Case
--------
This sink is used whenever you want to write to a ``PartitionedFileSet`` in ORC format
using a value from the record as a partition. For example, you might want to load historical
data from a database and partition the dataset on the original creation date of the data.


Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Dataset Name** | **Y** | None| Name of the ``PartitionedFileSet`` to which records are written. If it doesn't exist, it will be created. |
| **Schema** | **Y** | None | The schema of the record being written to the sink as a JSON Object. |
| **Partition Field Names** | **Y** | None | One or more fields that will be used to partition the dataset. |
| **Dataset Base Path** | **N** | [Namespace]/data/[Dataset name] | Base path for the ``PartitionedFileSet``. Defaults to the name of the dataset. |
| **Compression Codec** | **N** | None | Optional parameter to determine the compression codec to use on the resulting data. Valid values are None, Snappy, and zlib. |
| **Append to Existing Partition** | **N** | No | Allow appending to existing partitions, by default this capability is disabled.|
| **Compression Chunk Size** | **N** | None | Required if setting compressionCodec. Number of bytes in each compression chunk. |
| **Bytes per stripe** | **N** | None | Required if Compression Codec is set. Number of bytes in each stripe. |
| **Rows between index entries** | **N** | None | Required if Compression Codec is set. Number of rows between index entries (must be >= 1,000). |
| **Create inline indices** | **N** | True | Required if Compression Codec is set. Whether to create inline indices. |

Example
-------

For example, suppose the sink receives input records from customers and purchases:


    +==================================================================================================+
    | id | first_name | last_name |  street_address      |   city    | state | zipcode | purchase_date |  
    +==================================================================================================+
    | 1  | Douglas    | Williams  | 1, Vista Montana     | San Jose  | CA    | 95134   | 2009-01-02    |
    | 2  | David      | Johnson   | 3, Baypointe Parkway | Houston   | TX    | 78970   | 2009-01-01    |
    | 3  | Hugh       | Jackman   | 5, Cool Way          | Manhattan | NY    | 67263   | 2009-01-01    |
    | 4  | Walter     | White     | 3828, Piermont Dr    | Orlando   | FL    | 73498   | 2009-01-03    |
    | 5  | Frank      | Underwood | 1609 Far St.         | San Diego | CA    | 29770   | 2009-01-03    |
    | 6  | Serena     | Woods     | 123 Far St.          | Las Vegas | Nv    | 45334   | 2009-01-01    |
    +==================================================================================================+

If we choose ``purchase_date`` as a partition column field, the sink will create a ``PartitionedDataset`` and populate 
the partitions as follows:


    +==================================================================================================+
    | id | first_name | last_name |  street_address      |   city    | state | zipcode | purchase_date |  
    +==================================================================================================+
    | 2  | David      | Johnson   | 3, Baypointe Parkway | Houston   | TX    | 78970   | 2009-01-01    |
    | 3  | Hugh       | Jackman   | 5, Cool Way          | Manhattan | NY    | 67263   | 2009-01-01    |
    | 6  | Serena     | Woods     | 123 Far St.          | Las Vegas | Nv    | 45334   | 2009-01-01    |
    +==================================================================================================+

    +==================================================================================================+
    | id | first_name | last_name |  street_address      |   city    | state | zipcode | purchase_date |  
    +==================================================================================================+
    | 1  | Douglas    | Williams  | 1, Vista Montana     | San Jose  | CA    | 95134   | 2009-01-02    |
    +==================================================================================================+

    +==================================================================================================+
    | id | first_name | last_name |  street_address      |   city    | state | zipcode | purchase_date |  
    +==================================================================================================+
    | 4  | Walter     | White     | 3828, Piermont Dr    | Orlando   | FL    | 73498   | 2009-01-03    |
    | 5  | Frank      | Underwood | 1609 Far St.         | San Diego | CA    | 29770   | 2009-01-03    |
    +==================================================================================================+
    
