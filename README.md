bson-haskell [![Build Status](https://secure.travis-ci.org/selectel/bson-haskell.png)](http://travis-ci.org/selectel/bson-haskell)
------------

Haskell library for the encoding and decoding BSON documents, which are
JSON-like objects with a standard binary encoding, defined at
<http://bsonspec.org>. This library implements version 1 of that spec.

A BSON Document is an untyped (dynamically type-checked) record. I.e. it
is a list of name-value pairs, where a Value is a single sum type with
constructors for basic types (Bool, Int, Float, String, and Time),
compound types (List, and (embedded) Document), and special types
(Binary, Javascript, ObjectId, RegEx, and a few others).
