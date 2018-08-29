############
Introduction
############

""""""""""""""""
The UNIS Runtime
""""""""""""""""

The UNIS Runtime provides services for the maintenance, tracking, and
modification of documents held in data stores conforming to the UNIS schema.
The most important service provided by the Runtime is the conversion of
documents from their storage format into python objects.  These objects
behave as normal python objects with the distinction that their values are
initialized to reflect a corresponding document in a database.

Depending on the configuration of the Runtime, these documents may also have
a number of other useful properties; properties such as automatically changing
their values to reflect the document they correspond to and automatically
pushing changes to these object back to the document they correspond to.

New objects can also be inserted into the runtime, either as temporary *virtual*
documents or - by commiting the object to a data store - new perminent
documents.
