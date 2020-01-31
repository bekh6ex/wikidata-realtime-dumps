# Wikidata real-time dumps

Storing the state of Wikidata and updating it in "real-time" so every one can download the most recent state of data.

## Guaranties

TODO

## Public interface

TODO

## How does it work

Algorithm is based on [Actor Model](https://en.wikipedia.org/wiki/Actor_model) pattern.

The naming is based on archive metaphor. Archive consist of **Volumes** - each 
Volume contains a set of entity documents. Volume has an entity type, so one Volume 
can only contain entity documents of this type. Volume owns an entity document, so
one can be sure that a specific entity can be found in one Volume. Volumes a sorted, 
so that entity document `Q42` will always be before `Q43`. Volumes them selves have
sequential IDs, so all the documents in Volume `1` will have smaller IDs 
than all the documents in Volume `2`. Each volume has a start (like it starts from ID `Q12345`), 
but might not have an end yet, if it is the last one on this entity type.  

There are two main Actors:
  * **Volume keeper** - they are responsible for looking after their Volume contents. They put 
    new revisions in checking if by any accident they are trying to replace newer revision
    with the older one. Also they look after the size of their Volume not letting it becoming too big. 
    They are rather lazy, so they will persist the Volume when they see that there were no updates
    to their Volume, then and only then they will sit and write down everything they supposed to. 
    After that they will report about the accomplished job to their **Archivarius**.
  * **Archivarius** - they are responsible for maintaining the overall picture of what is happening with
    a single entity type: do we have all the data, was the data persisted. Also, they are masters 
    of their Volume Keepers. Their job is too important to waste their time on changing Volumes, or
    looking how others do it. 
    
### Archivarius - Volume Keeper interaction

So what Archivarius do is they know which one of the Volume Keepers is responsible
for a certain range of documents and will give then an order to update it. Volume Keeper on the other
hand wont hold the Archivarius back and force them to watch how they update the Volume. Instead,
they will silently knot about the task they received and put it in the pile of work they need to do.
When Volume Keeper finally can see that there is no more work coming for a while, they will take 
their pile of work and do it in one go. After that they will report what work did they do to their master.
Archivarius keeps track of what was done and when they receive a report from Volume Keeper they will 
cross off this work of their pending list and update the records of the current state.

What can also happen when all the actors are having a busy day, is Archivarius can give a command to update the Volume
with a new document to a Volume Keeper. But when Volume Keeper will be cleaning their pile of work they might 
notice that the Volume is too big already and this new document should go in another Volume. In this case Volume Keeper
will report this to the Archivarius and send the document back for it to be passed to another Volume Keeper.
