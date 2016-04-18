Sean Kelley
Samuel Yin

Select Optimizations:

1.Pushing Selections: We checked if we could apply a predicate a single table, and if it did we performed the
selection on it in order to reduce the amount of record. Also, for HashJoin, if the predicates applied to both 
tables we performed the selection. 

2.Join Ordering: When we first get the tables, we iterate through each of them and order them according
to their HeapFiles' record count, so that the smallest table would be first.

Potential Issue:
The only issue we had was closing iterators after invalid select queries. The way our Select.java code was
structured, we had some QueryChecking at the very end when testing a joined schema, and if the query was
invalid the exception would be thrown and thus we couldn't reach our statement at the end which closed all
iterators.

Everything should else should work as expected though.

