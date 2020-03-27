#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAll : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAll";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayHasAll>(context); }
    explicit FunctionArrayHasAll(const Context & context_) : FunctionArrayHasAllAny(context_, GatherUtils::ArrayHasKind::All, name) {}
};

void registerFunctionHasAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAll>();
}

}
