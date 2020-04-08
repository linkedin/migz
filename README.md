# MiGz

## Motivation

Compressing and decompressing files with standard compression utilities like gzip is a single-threaded affair.  For
large files on fast disks, that single thread becomes the bottleneck.

There are several utilities for multithreaded compression, including an extant Java library 
(https://github.com/shevek/parallelgzip), but no Java library (or GZip utility) also supports
multithreaded **de**compression, which is especially important for large files that are read repeatedly.  Hence, MiGz.

## Benefits

MiGz uses the GZip format, which has widespread support and offers both reasonably fast speed and a good compression 
ratio.

MiGz'ed files are also entirely valid GZip files, and can be read (single-threaded) by any GZip utility/library, 
including GZipInputStream!  Better still, MiGz'ed files can be multithreadedly decompressed by the MiGz decompressor.

On multicore machines, MiGz compression is *much* faster for any reasonably large file (tens of megabytes or more);
6x gains were seen on a MacBook with a large Wikipedia dump vs. the gzip command line utility (see Performance, below),
with only ~1% increase in file size vs. gzip at max compression.

Decompression is also sped up for larger files (many tens of megabytes or more); for smaller files, it's about the same
as Java's built-in single-threaded GZipInputStream.  Decompression of the aforementioned Wikipedia data was over 3x
faster.

## Performance

Using default settings on a MacBook Pro (with a SSD) with four hyperthreaded physical cores (8 logical cores):

### Shakespeare

The time to compress a 25.6MB collection of Shakespeare text was 25% that of GZip at max compression (~1.35s vs. ~6s),
with MiGz's output being ~1% larger.  However, the time to decompress, measured with the MUnzip command-line tool, is
~0.25s vs. GZip's ~0.09s, mostly attributable Java overhead: the time to decompress in Java with GZip is a slightly
faster ~0.23s.

Still, using the Java API, in a tight loop decompressing the same in-memory data 100 times and discarding the result,
the decompression time per copy is ~0.019s vs. ~0.073s for GZipStream.  We suspect that MiGz requires either some
JIT-related warm-up or amortizing the extra class loading cost vs. GZipStream before gains are seen on smaller files.

### German Wikipedia

This is an 18GB XML dump of German Wikipedia articles.  At maximum compression, MiGz compresses it in 198.2s, vs. 810.2s
for GZip.  Decompression is 15.6s for MiGz and 65.2s for GZip.  Compressed file size is roughly equal: 5.74GB for MiGz
and 5.70GB for GZip (a difference of less than 1%).


## Using MiGz in Java and other JVM Languages

MiGz is used just like you would use GZipInputStream and GZipOutputStream, with the analogous MiGzInputStream and 
MiGzOutputStream classes.  For example, decompression is as simple as:

```java
InputStream is = ...
MiGzInputStream mis = new MiGzInputStream(is);
```
Compression is just as simple:

```java
OutputStream os = ...
MiGzOutputStream mos = new MiGzOutputStream(os);
```
## Using MiGz from the Command-line 

The MiGz project also comes with modules for two simple command-line tools; you may build these yourself or use our
precompiled executables (for *nix platforms) or JARs (other platforms).

### mzip

mzip uses MiGz to compresses data from stdin and outputs the compressed data to stdout.  For example, to compress 
data.txt and write the result to data.gz, we can run:

```bash
mzip < data.txt > data.gz
```
### munzip

munzip likewise uses MiGz to decompress data from stdin and output the original, uncompressed data to stdout.  For 
example, to decompress data.gz back to data.txt:

```bash
muzip < data.gz > data.txt
```
## Recommended settings

The default block size is 512KB, which provides good speed (smaller block sizes -> better parallelization) on
relatively "small" (10s of MB) files, while still maintaining file sizes very close to standard gzip, though you can
reduce block size to ~100KB before the difference is really noticeable.

The default thread count is either the number of logical cores on your machine (decompression) or twice that
(compression).  Extra threads are use for compression because MiGz uses the threads to effectively buffer the
output without using a dedicated writer thread.  However, this may change in the future and we recommend sticking with
the default thread count as "future proofing".

## Building MiGz
To build the MiGz Java library, use the command `gradle :migz:build`.
To build the munzip tool, use the command `gradle :munzip:build`.
To build the mzip tool, use the command `gradle :mzip:build`.