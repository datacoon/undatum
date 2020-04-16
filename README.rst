undatum: a command-line tool for data processing
################################################

undatum (pronounced *un-da-tum*) is a command line data processing tool.
Its goal is to make CLI interaction with huge datasets so easy as possible.
It provides a simple ``undatum`` command that allows to convert, split, calculate frequency, statistics and to validate
data in CSV, JSON lines, BSON files.


.. contents::

.. section-numbering::



Main features
=============


* Common data operations against CSV, JSON lines and BSON files
* Built-in data filtering
* Conversion between CSV, JSONl, BSON, XML, XLS, XLSX file types
* Low memory footprint
* Support for compressed datasets
* Advanced statistics calculations
* Date/datetime fields automatic recognition
* Data validation
* Documentation
* Test coverage




Installation
============


macOS
-----


On macOS, undatum can be installed via `Homebrew <https://brew.sh/>`_
(recommended):

.. code-block:: bash

    $ brew install undatum


A MacPorts *port* is also available:

.. code-block:: bash

    $ port install undatum

Linux
-----

Most Linux distributions provide a package that can be installed using the
system package manager, for example:

.. code-block:: bash

    # Debian, Ubuntu, etc.
    $ apt install undatum

.. code-block:: bash

    # Fedora
    $ dnf install undatum

.. code-block:: bash

    # CentOS, RHEL, ...
    $ yum install undatum

.. code-block:: bash

    # Arch Linux
    $ pacman -S undatum


Windows, etc.
-------------

A universal installation method (that works on Windows, Mac OS X, Linux, …,
and always provides the latest version) is to use pip:


.. code-block:: bash

    # Make sure we have an up-to-date version of pip and setuptools:
    $ pip install --upgrade pip setuptools

    $ pip install --upgrade undatum


(If ``pip`` installation fails for some reason, you can try
``easy_install undatum`` as a fallback.)


Python version
--------------

Python version 3.6 or greater is required.



Usage
=====


Synopsis:

.. code-block:: bash

    $ undatum [flags] [command] inputfile


See also ``undatum --help``.


Examples
--------

Get headers from file as `headers command`_,  `JSONl`_ data:

.. code-block:: bash

    $ undatum headers examples/ausgovdir.jsonl


Analyze file and generate statistics `stats command`_:

.. code-block:: bash

    $ undatum stats examples/ausgovdir.jsonl


Get `frequency command`_ of values for field GovSystem in the list of Russian federal government domains from  `govdomains repository <https://github.com/infoculture/govdomains/tree/master/refined>`_

.. code-block:: bash

    $ undatum frequency examples/feddomains.csv --fields GovSystem


Get all unique values using `uniq command`_ of the *item.type* field

.. code-block:: bash

    $ undatum uniq --fields item.type examples/ausgovdir.jsonl

`convert command`_ from XML to JSON lines file on tag *item*:

.. code-block:: bash

    $ undatum convert --tagname item examples/ausgovdir.xml examples/ausgovdir.jsonl


Validate data with `validate command`_ against validation rule *ru.org.inn* and field *VendorINN* in  data file. Output is statistcs only :

.. code-block:: bash

    $ undatum validate -r ru.org.inn --mode stats --fields VendorINN examples/roszdravvendors_final.jsonl > inn_stats.json

Validate data with `validate command`_ against validation rule *ru.org.inn* and field *VendorINN* in  data file. Output all invalid records :

.. code-block:: bash

    $ undatum validate -r ru.org.inn --mode invalid --fields VendorINN examples/roszdravvendors_final.jsonl > inn_invalid.json

Commands
========

Frequency command
-----------------
Field value frequency calculator. Returns frequency table for certain field

Get frequencies of values for field *GovSystem* in the list of Russian federal government domains from  `govdomains repository <https://github.com/infoculture/govdomains/tree/master/refined>`_

.. code-block:: bash

    $ undatum frequency examples/feddomains.csv --fields GovSystem




Uniq command
-------------

Returns all unique files of certain field(s). Accepts parameter *fields* with comma separated fields to gets it unique values.
Provide single field name to get unique values of this field or provide list of fields to get combined unique values.


Returns all unique values of field *regions* in selected JSONl file

.. code-block:: bash

    $ undatum uniq --fields region examples/reestrgp_final.jsonl

Returns all unique combinations of fields *status* and *regions* in selected JSONl file

.. code-block:: bash

    $ undatum uniq --fields status,region examples/reestrgp_final.jsonl


Convert command
---------------

Converts data from one format to another.
Supports conversions:

* XML to JSON lines
* CSV to JSON lines
* XLS to JSON lines
* XLSX to JSON lines
* XLS to CSV
* CSV to BSON
* XLS to BSON

Conversion between XML and JSON lines require flag *tagname* with name of tag which should be converted into single JSON record.

Converts XML ausgovdir.xml with tag named *item* to ausgovdir.jsonl

.. code-block:: bash

    $ undatum convert --tagname item examples/ausgovdir.xml examples/ausgovdir.jsonl


Validate command
----------------

*Validate* command used to check every value of of field against validation rules like rule to validate email or url.

Current supported rules:

* *common.email* - checks if value is email
* *common.url* - checks if value is url
* *ru.org.inn* - checks if value is russian organization INN identifier
* *ru.org.ogrn* - checks if value if russian organization OGRN identifier

Validate data with `validate command`_ against validation rule *ru.org.inn* and field *VendorINN* in  data file. Output all invalid records :

.. code-block:: bash

    $ undatum validate -r ru.org.inn --mode invalid --fields VendorINN examples/roszdravvendors_final.jsonl > inn_invalid.json


Headers command
---------------
Returns fieldnames of the file. Supports CSV, JSON, BSON file types.
For CSV file it takes first line of the file and for JSON lines and BSON files it processes number of records provided as *limit* parameter with default value 10000.

Returns headers of JSON lines file with top 10 000 records (default value)

.. code-block:: bash

    $ undatum headers examples/ausgovdir.jsonl


Returns headers of JSON lines file using top 50 000 records

.. code-block:: bash

    $ undatum headers --limit 50000 examples/ausgovdir.jsonl

Stats command
-------------
Collects statistics about data in dataset. Right now supports only JSON lines files

Returns table with following data:

* *key* - name of the key
* *ftype* - data type of the values with this key
* *is_dictkey* - if True, than this key is identified as dictionary value
* *is_uniq* - if True, identified as unique field
* *n_uniq* - number of unique values
* *share_uniq* - share of unique values among all values
* *minlen* - minimal length of the field
* *maxlen* - maximum length of the field
* *avglen* - average length of the field

Returns stats for JSON lines file

.. code-block:: bash

    $ undatum stats examples/ausgovdir.jsonl

Analysis of JSON lines file and verifies each field that it's date field, detects date format:

.. code-block:: bash

    $ undatum stats --checkdates examples/ausgovdir.jsonl



Split command
-------------
Splits dataset into number of datasets based on number of records or field value.
Chunksize parameter *-c* used to set size of chunk if dataset should be splitted by chunk size rule.
If dataset should be splitted by field value than *--fields* parameter used.

Split dataset as 10000 records chunks, procuces files like filename_1.jsonl, filename_2.jsonl where *filename* is name of original file except extension.

.. code-block:: bash

    $ undatum split -c 10000 examples/ausgovdir.jsonl


Split dataset as number of files based of field *item.type", generates files [filename]_[value1].jsonl, [filename]_[value2].jsonl and e.t.c.
There are *[filename]* - ausgovdir and *[value1]* - certain unique value from *item.type* field

.. code-block:: bash

    $ undatum split --fields item.type examples/ausgovdir.jsonl



Select command
--------------

Select or re-order columns from file. Supports CSV, JSON lines, BSON

Returns columns *item.title* and *item.type* from ausgovdir.jsonl

.. code-block:: bash

    $ undatum select --fields item.title,item.type examples/ausgovdir.jsonl


Returns columns *item.title* and *item.type* from ausgovdir.jsonl and stores result as selected.jsonl

.. code-block:: bash

    $ undatum select --fields item.title,item.type -o selected.jsonl examples/ausgovdir.jsonl

Flatten command
---------------

Flatten data records. Write them as one value per row

Returns all columns as flattened key,value

.. code-block:: bash

    $ undatum flatten examples/ausgovdir.jsonl


Advanced
========

Filtering
---------

You could filter values of any file record by using *filter* attr for any command where it's suported.

Returns columns item.title and item.type filtered with *item.type* value as *role*. Note: keys should be surrounded by "`" and text values by "'".

.. code-block:: bash

    $ undatum select --fields item.title,item.type --filter "`item.type` == 'role'" examples/ausgovdir.jsonl

Data containers
---------------

Sometimes, to keep keep memory usage as low as possible to process huge data files.
These files are inside compressed containers like .zip, .gz, .bz2 or .tar.gz files.
*undatum* could process compressed files with little memory footprint, but it could slow down file processing.

Returns headers from subs_dump_1.jsonl file inside subs_dump_1.zip file. Require parameter *-z* to be set and *--format-in* force input file type.

.. code-block:: bash

    $ undatum headers --format-in jsonl -z subs_dump_1.zip


Date detection
--------------
JSON, JSON lines and CSV files do not support date and datetime data types.
If you manually prepare your data, than you could define datetime in JSON schema for example.B
But if data is external, you need to identify these fields.

undatum supports date identification via `qddate <https://github.com/ivbeg/qddate>`_ python library with automatic date detection abilities.

.. code-block:: bash

    $ undatum stats --checkdates examples/ausgovdir.jsonl


Data types
==========

JSONl
-----

JSON lines is a replacement to CSV and JSON files, with JSON flexibility and ability to process data line by line, without loading everithing into memory.
