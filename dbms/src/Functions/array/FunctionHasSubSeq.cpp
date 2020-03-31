#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasSubSeq : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasSubSeq";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayHasSubSeq>(context); }
    explicit FunctionArrayHasSubSeq(const Context & context_) : FunctionArrayHasAllAny(context_, GatherUtils::ArraySearchType::SubSeq, name) {}
};

void registerFunctionHasSubSeq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasSubSeq>();
}

}
