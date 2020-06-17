#pragma once

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Parsers/StringRange.h>

namespace DB
{
class CompressionCodecTrie : public ICompressionCodec
{
public:
    CompressionCodecTrie(DataTypePtr data_type_) :
        log(&Poco::Logger::get("CompressionCodecTrie")), data_type(data_type_) { }

    uint8_t getMethodByte() const override;

    String getCodecDesc() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    using KeysInBlock = std::vector<String>;

    Poco::Logger * log;

    DataTypePtr data_type;

    void deserializeKeysFromBlockString(ReadBuffer & istr, KeysInBlock & keysInBlock) const;
    // todo
//    void deserializeKeysFromBlockDataTypeArray(ReadBuffer & istr, KeysInBlock & keysInBlock) const;

};

class CompressionCodecFactory;
void registerCodecTrie(CompressionCodecFactory & factory);
};
