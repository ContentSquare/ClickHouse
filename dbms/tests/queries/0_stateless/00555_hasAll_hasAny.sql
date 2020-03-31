select hasAll([], []);
select hasAll([], [1]);
select hasAll([], [NULL]);
select hasAll([Null], [Null]);
select hasAll([Null], [Null, 1]);
select hasAll([1], []);
select hasAll([1], [Null]);
select hasAll([1, Null], [Null]);
select '-';

select hasAny([], []);
select hasAny([], [1]);
select hasAny([], [NULL]);
select hasAny([Null], [Null]);
select hasAny([Null], [Null, 1]);
select hasAny([1], []);
select hasAny([1], [Null]);
select hasAny([1, Null], [Null]);
select '-';

select hasSubSeq([], []);
select hasSubSeq([], [1]);
select hasSubSeq([], [NULL]);
select hasSubSeq([Null], [Null]);
select hasSubSeq([Null], [Null, 1]);
select hasSubSeq([1], []);
select hasSubSeq([1], [Null]);
select hasSubSeq([1, Null], [Null]);
select '-';


select hasAll([1], emptyArrayUInt8());
select hasAny([1], emptyArrayUInt8());
select hasSubSeq([1], emptyArrayUInt8());
select '-';

select hasAny([1, 2, 3, 4], [5, 6]);
select hasAny([1, 2, 3, 4], [1, 3, 5]);
select hasAny([1, 2, 3, 4], [1, 3]);
select hasAll([1, 2, 3, 4], [1, 3]);
select hasAll([1, 2, 3, 4], [1, 3, 5]);
select hasSubSeq([1, 2, 3, 4], [1, 3]);
select hasSubSeq([1, 2, 3, 4], [1, 3, 5]);
select hasAny([-128, 1., 512], [1.]);
select hasAny([-128, 1.0, 512], [.3]);
select hasAll([-128, 1.0, 512], [1.0]);
select hasAll([-128, 1.0, 512], [1.0, 513]);
select hasSubSeq([-128, 1., 512], [1.]);
select hasSubSeq([-128, 1.0, 512], [.3]);
select '-';

select hasAny(['a'], ['a']);
select hasAll(['a'], ['a']);
select hasSubSeq(['a'], ['a']);
select hasAny(['a', 'b'], ['a', 'c']);
select hasAll(['a', 'b'], ['a', 'c']);
select hasSubSeq(['a', 'b'], ['a', 'c']);
select '-';

select hasAny([1], ['a']);
select hasAll([1], ['a']);
select hasSubSeq([1], ['a']);
select hasAll([[1, 2], [3, 4]], ['a', 'c']);
select hasAny([[1, 2], [3, 4]], ['a', 'c']);
select hasSubSeq([[1, 2], [3, 4]], ['a', 'c']);
select '-';

select hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]]);
select hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]]);
select hasAny([[1, 2], [3, 4]], [[1, 2], [3, 5]]);
select hasAny([[1, 2], [3, 4]], [[1, 3], [4, 2]]);
select hasAny([[1, 2], [3, 4]], [[1, 2], [3, 4], [5, 6]]);
select hasAny([[1, 2], [3, 4]], [[1, 3], [4, 2]]);

