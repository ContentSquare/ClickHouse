#include <Compression/CompressionCodecTrie.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/hex.h>

namespace DB
{
UInt32 CompressionCodecTrie::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    //todo this will change once radix tree is integrated
    memcpy(dest, source, source_size);
    ReadBufferFromMemory istr(source, source_size);
    std::vector<String> keys;

    auto type_index = data_type->getTypeId();
    if (type_index == TypeIndex::String)
    {
        this->deserializeKeysFromBlockString(istr, keys);
    }
    else if (type_index == TypeIndex::Array)
    {
        auto data_type_array = std::dynamic_pointer_cast<const DataTypeArray>(data_type);
        assert(data_type_array->getNestedType()->getTypeId() == TypeIndex::String);
        //todo
        //this->deserializeKeysFromBlockDataTypeArray(istr, keys);
    }
    else
    {
        throw Exception("Not supported data type", 987);
    }

    auto str = std::string("Found key is: ");

    for (const auto & key : keys)
    {
        LOG_DEBUG(log, str + key);
    }

    return source_size;
}
void CompressionCodecTrie::doDecompressData(const char * source, UInt32 /*source_size*/, char * dest, UInt32 uncompressed_size) const
{
    memcpy(dest, source, uncompressed_size);
}
String CompressionCodecTrie::getCodecDesc() const
{
    return "TRIE";
}
uint8_t CompressionCodecTrie::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::TRIE);
}
void CompressionCodecTrie::deserializeKeysFromBlockString(ReadBuffer & istr, CompressionCodecTrie::KeysInBlock & keysInBlock) const
{
    while (!istr.eof())
    {
        Field field_rows;
        data_type->deserializeBinary(field_rows, istr);
        keysInBlock.push_back(field_rows.get<String>());
    }
}
/**
void CompressionCodecTrie::deserializeKeysFromBlockDataTypeArray(ReadBuffer & istr, CompressionCodecTrie::KeysInBlock & keysInBlock) const
{

    size_t size;
    readVarUInt(size, istr);
    auto field = Array(size);
    Array & arr = get<Array &>(field);
    for (size_t i = 0; i < size; ++i)
        nested->deserializeBinary(arr[i], istr);

}
*/
/**
 * @param column_type needs to be of DataTypeArray kind
 */
bool assertArrayNestedString(DataTypePtr column_type)
{
    auto data_type_array = std::dynamic_pointer_cast<const DataTypeArray>(column_type);
    return data_type_array->getNestedType()->getTypeId() == TypeIndex::String;
}

void registerCodecTrie(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::TRIE);
    factory.registerCompressionCodecWithType("TRIE", method_code, [&](const ASTPtr &, DataTypePtr column_type) -> CompressionCodecPtr {
        auto typeId = column_type->getTypeId();
        auto isArray = typeId == TypeIndex::Array;
        auto isString = typeId == TypeIndex::String;

        auto isOk = isArray || isString;

        if (isArray)
            isOk = assertArrayNestedString(column_type);

        if (!isOk)
            throw Exception("Not supported data type", 987);
        else
            return std::make_shared<CompressionCodecTrie>(column_type);
    });
}

}
