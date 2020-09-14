Monday 14-Sep-2020

*stand-alone-testing-of-alt-sql-from-proc-solns — high-level overview*

This directory tree holds the programs and data that implement a stand-alone study of alternative implementations for the various use cases that are specified for TPC-C submissions, here:

```
TPC BENCHMARK Standard Specification Revision 5.11
http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
```

As of this document's date, only the _"New Order"_ use case has been investigated— and this is still a work-in-progress. However, this use case is sufficiently rich that it can be taken as a paradigm for other use cases.

*New Order Overview*

The use case is conveniently illustrated by a pseudo code description of a procedure that implements it. This is presented in "_Appendix A: Sample Programs"_ in the section _"A.1 The New-Order Transaction"_ of the TPC-C Specification. (The definitive specification is given in prose elsewhere in the TPC-C document.)

This high-level account is sufficient:

- The procedure's input is a few scalars, like the ID of the customer that places the order and the ID of the so-called "home warehouse" from which the ordered items might be fulfilled, together with a compound value, conveniently pictured as a _list of tuples_ (in Python-speak) or an _array of records_ (in PostgreSQL-speak). Each tuple represents a single to-be-ordered item as three scalars: the ID of the to-be-ordered item, the ID of the _actual_ warehouse from which the ordered item will be fulfilled, and the quantity of the item that is to be ordered. The typical cardinality of the list is ten items. But, because the values are chosen by a pseudo-random scheme the the TPC-C specification describes, this number is subject to a fair amount of variability.
- The procedure must implement its whole purpose as a single transaction (as the term is conventionally used in RDBMS-speak).
- The procedure must acquire salient facts (like the unit price for an item, the customer discount, and so on); it must establish the order number by pulling this from a central transactional oracle that it then updates for the next pull; it must decrement the quantity on hand in the relevant warehouse—implementing a simple rule that increments the quantity on hand by a fixed globally constant amount upon depletion; and it must record the full details of the new order in a conventional parent-child table pair.

The Appendix A.1 pseudocode describes the procedure using a naïve mix of procedural code where the list of tuples is processed iteratively and where only single-row SQLs (_selects_, _inserts_, and _updates_—with no _deletes_) are used. But it is careful to say that actual submissions may use what they decide is optimal—making a nod towards the use of set-based SQLs.

*Starting point for this study*

The _New Order_ implementation was presented as a stored procedure (implemented using PL/pgSQL) that followed the TPC-C's pseudocode fairly closely by avoiding the use of set-based SQLs and by using two successive _selects_ from different tables when a single _select_ from a two-table join is semantically sufficient.

The code was accompanied by sample data with row-counts between about 100K and 300K for the larger tables.

*Scope of the study*

The study focused on these two performance comparisons:

- The relative speeds of a stored procedure encapsulation of the procedural-SQL hybrid and an equivalent client-side implementation. (Python was used in this study.)
- The relative speeds of the naïve hybrid of iterative procedural code and single row SQLs, and a semantically equivalent hybrid where the procedural code uses no iteration but pushes down the effect beneath the SQL implementation by using set-based SQLs.

Long-established received wisdom for applications that use a monolithic SQL database (especially like Oracle Database) rests on these two orthogonal axioms:

- a stored procedure encapsulation, initiated by a single server call, brings a dramatic performance benefit w.r.t. a semantically equivalent implementation implemented in client-side procedural code that issues a series of top-level SQLs.
- an approach that uses set-based SQL to the fullest extent that SQL semantics allows, and that uses the procedure features of the harness that, inevitably, must be used to issue all the SQLs that are needed is dramatically faster than a semantically equivalent procedural-SQL hybrid that uses only single-row SQLs.

Here is a frequently referenced video recording that presents an empirical investigation, using Oracle Database, that investigates these sizes of the performance differences between different approach. It shows that the approach that uses a stored procedure encapsulation of set-based SQLs is about two orders of magnitude faster than the approach that uses client-side procedural code and only single-row SQLs.

```
https://www.youtube.com/watch?v=8jiJDflpw4Y
```

*Present state of the study*

- The sought-after approach that uses set-based SQLs has been described and then implemented both in PL/pgSQL and in Python.

- The starting-point naïve PL/pgSQL implementation has been transcribed into Python.

- A correctness testing harness has been implemented. This shows that all four approaches implement the identical effect. Moreover, all correctness tests have been run both using vanilla PostgreSQL on a top-of-the range MacBook and a single-node YugabyteDB Version YB-2.2.0.0 cluster on the same machine (under the current latest MacOS Mojave).

- A careful stand-alone experiment has been designed and implemented to investigate the performance difference between the use of Python to issue many single-row SQLs and the use of PL/pgSQL to implement the same schedule of SQLs.

*Performance results to date*

The stand-alone experiment has shown that, to first order, the stored procedure approach does deliver its expected benefit using vanilla PostgreSQL. Briefly, the speeds are so fast up to about five single-row SQLs that reliable speed ratios are hard to measure. But the speeds are very similar. Thereafter, stored procedure encapsulation starts to deliver its expected benefit up to about 265 single-row SQLs. (The numbers of SQLs were increased by 2x with each step from 1 through 512.) Strangely, the proc:sql speed ratio drops going from 256 to 512. This needs investigation—for example, by watching the under-the-covers execution of PostgreSQL.

The identical experiments, using YugabyteDB show dumbfoundingly unexpected results. For example, in some tests the stored procedure encapsulation is 100x slower than using client-side code for small numbers of single-row SQLs; and it rises to only as "much" as ~80% of the client speed for large numbers of single-row SQLs. This needs immediate and serious investigation.

So far, the timing harness that was developed for these stand-alone tests has yet to be implemented for the four competing implementations of _New Order_. This will take trivial effort. Preliminary testing of just the stored procedure approaches, using server-side timing, show two kinds of surprise:

- For a smallish number of repetitions, the naïve approach and the set-based SQL approach show them to have about the same speeds (both using PostgreSQL and using YugabyteDB).

- As the number of repetitions increases, so does the elapsed time per repetition. This is seen both using PostgreSQL and using YugabyteDB. This seems to suggest a memory leak—presumably in the PostgreSQL code.

*External documentation*

So far, none has been written. However, an oral presentation of the code has been made to other members of the TPC-C project team.

As a stop gap, the manually pruned output from `tree` is here:

```
stand-alone-testing-of-alt-sql-from-proc-solns-tree.txt
```

on the same top directory as this _README.md_. It gives an overview of how the code is organized.