#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayHasSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, ArrayHasKind & kind, ColumnUInt8 & result)
    {
        switch (kind){
            case ArrayHasKind::All:
                arrayAllAny<ArrayHasKind::All>(first, second, result);
                break;
            case ArrayHasKind::Any:
                arrayAllAny<ArrayHasKind::Any>(first, second, result);
                break;
            case ArrayHasKind::SubSeq:
                arrayAllAny<ArrayHasKind::SubSeq>(first, second, result);
                break;

        }
    }
};


void sliceHas(IArraySource & first, IArraySource & second, ArrayHasKind & kind, ColumnUInt8 & result)
{
    ArrayHasSelectArraySourcePair::select(first, second, kind, result);
}

}
