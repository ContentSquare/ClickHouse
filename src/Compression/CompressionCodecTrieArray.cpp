#include "CompressionCodecTrieArray.h"
#include <Compression/CompressionCodecTrieString.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/hex.h>

namespace DB
{
UInt32 CompressionCodecTrieArray::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    //todo this will change once radix tree is integrated
    memcpy(dest, source, source_size);
    ReadBufferFromMemory istr(source, source_size);
    KeysInAllArraysBlockWithSize allArrays;

    std::vector<String> keys;
    this->deserializeKeysFromBlockString(istr,keys);
//    this->deserializeKeysFromBlockDataTypeArray(istr, allArrays);
//    printFoundArrays(allArrays);
    LOG_DEBUG(log, std::string("Keys in array:") );
    printFoundKeys(keys);
    return source_size;
}
void CompressionCodecTrieArray::doDecompressData(const char * source, UInt32 /*source_size*/, char * dest, UInt32 uncompressed_size) const
{
    memcpy(dest, source, uncompressed_size);
}
String CompressionCodecTrieArray::getCodecDesc() const
{
    return "TrieArray";
}
uint8_t CompressionCodecTrieArray::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::TRIE_ARRAY);
}

void CompressionCodecTrieArray::deserializeKeysFromBlockDataTypeArray(
    ReadBuffer & istr, CompressionCodecTrieArray::KeysInAllArraysBlockWithSize & allArrays) const
{
    while (!istr.eof())
    {
        size_t size;
        readVarUInt(size, istr);
        if (size > 0)
        {
            KeysInBlock keysInBlock(size);
            String s = std::string{};
            UInt64 sizeNested;
            for (size_t i = 0; i < size; ++i){
                s.clear();
                readVarUInt(sizeNested, istr);
                s.resize(sizeNested);
                istr.readStrict(s.data(), size);
                keysInBlock.push_back(s);
            }
//                this->deserializeKeysFromBlockString(istr, keysInBlock);
            KeysInArrayBlockWithSize pair(size, keysInBlock);
            allArrays.push_back(std::move(pair));
        }
    }
}

void CompressionCodecTrieArray::printFoundArrays(
    CompressionCodecTrieArray::KeysInAllArraysBlockWithSize & keysInAllArraysBlockWithSize) const
{
    for (const auto & pair : keysInAllArraysBlockWithSize)
    {
        LOG_DEBUG(log, std::string("Size of found array: ") + std::to_string(pair.first));
        printFoundKeys(const_cast<KeysInBlock &>(pair.second));
    }
}

void registerCodecTrieArray(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::TRIE_ARRAY);
    factory.registerSimpleCompressionCodec("TrieArray", method_code, [&]() { return std::make_shared<CompressionCodecTrieArray>(); });
}

}
