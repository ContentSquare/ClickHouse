#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAny : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAny";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayHasAny>(context); }
    explicit FunctionArrayHasAny(const Context & context_) : FunctionArrayHasAllAny(context_, GatherUtils::ArrayHasKind::Any, name) {}
};

void registerFunctionHasAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
