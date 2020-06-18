#include "CompressionCodecTrieArray.h"
#include <Compression/CompressionCodecTrieString.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/hex.h>

namespace DB
{
UInt32 CompressionCodecTrieArray::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    // beware all of this code here is copy paste from CompressionCodecTrieString
    // sorry for that xD
    ReadBufferFromMemory istr(source, source_size);

    std::vector<String> keys;
    this->deserializeKeysFromBlockString(istr,keys);
    LOG_DEBUG(log, std::string("Keys in array:") );
    printFoundKeys(keys);

    auto row_number = 0;
    Trie trie;
    for (const auto & key : keys)
    {
        trie.insert(key, row_number++);
    }

    auto radix_tree = trie.createRadixTree();
    auto serialized_radix_tree = serde_ptr->serialize(radix_tree);
    auto serialized_radix_size = serialized_radix_tree.size();

    LOG_DEBUG(log, std::string("Number of nodes of radix after compressing array: ") + std::to_string(radix_tree.countNodes()));
    LOG_DEBUG(log, std::string("Number of rows after compressing array: ") + std::to_string(radix_tree.countRows()));
    memcpy(dest, serialized_radix_tree.data(), serialized_radix_size);
    return serialized_radix_size;
//    return source_size;
}
void CompressionCodecTrieArray::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    // todo this gotta be improved
    auto raw_data = String(source, source_size);

    auto radix_tree = serde_ptr->deserialize(raw_data);
    std::vector<String> lines;
    radix_tree.generateLines(lines);

    LOG_DEBUG(log, std::string("Number of nodes of radix after decompressing array: ") + std::to_string(radix_tree.countNodes()));
    LOG_DEBUG(log, std::string("Number of rows after decompressing array: ") + std::to_string(radix_tree.countRows()));

    // we have to encode vector of found lines into buffer of characters
    WriteBuffer writeBuffer(&dest[0], uncompressed_size);

    for (const auto & line : lines)
    {
        auto size = static_cast<UInt64>(line.size());
        writeVarUInt(size, writeBuffer);
        writeString(line.data(), writeBuffer);
    }
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
