# Wikidata real-time dumps

Storing the state of Wikidata and updating it in "real-time" so every one can download the most recent state of data.

## Guaranties and limitations

  * Entities in dump will always be sorted. `Q1` always comes before `Q8`.
  
  * The data in the dump might be (and probably will be) inconsistent. Entities that are later in dump
    will have more recent versions that the one that are in the beginning. This happens 
    because the response is being streamed by pieces (Volumes) and consistency exists within one volume only.
    Also, although some the entities will be created in Wikidata during the download, they might not be present in the
    downloaded dump, due to technical reasons. 
    
  * It is not possible to resume download from the exact byte position. If download process fails, 
    it has to be restarted or dump can be downloaded in pieces, but this has to be planned in advance. *TODO*
    
  * Delay between dump contents and data in Wikidata will be at lease as big as the delay 
    between data change in Wikidata and event appearing in [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).
    

## Public interface

TODO

Dump format is new [line delimited JSON](http://ndjson.org/) in compressed with `gzip`.

## How does it work

Algorithm is based on [Actor Model](https://en.wikipedia.org/wiki/Actor_model) pattern.

The naming is based on archive metaphor. Archive consist of **Volumes** - each 
Volume contains a set of entity documents. Volume has an entity type, so one Volume 
can only contain entity documents of this type. Volume owns an entity document, so
one can be sure that a specific entity can be found in exactly one Volume. Volumes are sorted, 
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

*Not fully implemented yet*

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

When the dump for certain entity type is requested by a client, Archivarius will receive the request
and return stream of pieces of data, each one provided by the responsible Volume Keeper. 
Each piece will be read only when the client have received all the previous pieces (maybe a a bit before).

### Lifecycle

#### Initialization

*It works the same entity types the same, to simplify let's take items only.*

When application is started, the Archivarius for items will be started, who will load it's state (if there is one)
and based on state will start a number of Volume Keepers to look for existing Volumes (or at least one 
if the state is fresh).

Then the initialization starts. First we will ask Archivarius which is the biggest entity ID they together 
with Volume Keepers have managed to store.
We will send the current latest EventId from [Event Streams] to Archivarius. Archivarius will either store it 
for later use or ignore it if they know that other data they responsible for is dated earlier than this event.
After that we will look up the ID of the current latest created item in Wikidata and start a stream of IDs 
from the one seen by Archivarius till the latest. For each ID we will get the entity from Wikidata API 
(or from dump TODO) and if it exists send it to the Archivarius to be stored.

After initialization is finished, the Archivarius will be notified about it as well.

*Initialization does not need to happen on every application start.*

#### Regular workflow

After Archivarius is initialized they will be notified about the events from the [Event Stream] and
as usual delegate the handling to responsible Volume Keepers awaiting their reports and tracking what was persisted, 
what was not and when and what can be marked as persisted to be 100% sure to survive emergency application restart.

Volume Keepers will check IDs and revisions of entities to be sure that they won't accidentally overwrite new data with 
the old one.  
