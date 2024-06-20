# Build your Own Database From Srcatch

## step1
定义node结构，给出每个字节区间含义，如元数据区和数据存储区，在此基础上提供元数据读取和写入方法、数据的读取和写入方法。

## step2
数据如何组织，使用bTree。
本文使用B+树来保存所有数据，因此需要定义一个bTree结构，在bTree结构上增加数据读取、删除、写入的方法。bTree上的页的操作留给上层结构去实现。
在这一步可以页的操作可以直接使用内存进行自测bTee上数据。

## step3
实现持久化操作、空闲页管理、脏页回收，在step2基础上提供了页的具体操作。
在这一步基本实现了一个完整带有持久化操作的kv数据库

## step4
实现一个relationDB,新增了表结构

## step5
range query

## step6
secondary index

## step7
transaction and concurrent

## step8
query language