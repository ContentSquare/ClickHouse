#pragma once

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Parsers/StringRange.h>
#include <Compression/CompressionCodecTrieString.h>

namespace DB
{
class CompressionCodecTrieArray : public CompressionCodecTrieString
{
public:
    CompressionCodecTrieArray() : log(&Poco::Logger::get("CompressionCodecTrieArray")) { }

    uint8_t getMethodByte() const override;

    String getCodecDesc() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    using KeysInArrayBlockWithSize = std::pair<UInt64, KeysInBlock>;
    using KeysInAllArraysBlockWithSize = std::vector<KeysInArrayBlockWithSize>;

    Poco::Logger * log;

    void deserializeKeysFromBlockDataTypeArray(ReadBuffer & istr, KeysInAllArraysBlockWithSize & allArrays) const;

    void printFoundArrays(KeysInAllArraysBlockWithSize & keysInAllArraysBlockWithSize) const;
};

class CompressionCodecFactory;
void registerCodecTrieArray(CompressionCodecFactory & factory);
};
