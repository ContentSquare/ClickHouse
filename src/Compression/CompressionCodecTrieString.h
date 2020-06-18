#pragma once

#include <set>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Parsers/StringRange.h>

namespace DB
{
static int RLE_ACTIVATION_FLAG = 0;

class CompressionCodecTrieString : public ICompressionCodec
{
public:
    CompressionCodecTrieString() : serde_ptr(std::make_unique<Serde>()), log(&Poco::Logger::get("CompressionCodecTrieString")) { }

    uint8_t getMethodByte() const override;

    String getCodecDesc() const override;

protected:
    using KeysInBlock = std::vector<String>;

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    virtual void deserializeKeysFromBlockString(ReadBuffer & istr, KeysInBlock & keysInBlock) const;

    virtual void printFoundKeys(KeysInBlock & keys) const;

    class RadixTrie
    {
    public:
        RadixTrie() = default;
        RadixTrie(String radixTreesuffix_, bool hasWord_, std::vector<int> rows_)
            : radix_treesuffix(radixTreesuffix_), has_word(hasWord_), rows(rows_)
        {
        }
        void showChildrenKeys();
        void makeRadixTree();
        int countNodes();
        int countRows();
        std::set<String> getDistinctKeys();
        bool radixSearch(const String &);
        std::string radix_treesuffix = "";
        bool has_word = false;
        std::vector<RadixTrie> children;
        std::vector<int> rows;
        void generateLines(std::vector<String> & lines);

    private:
        void generateLinesRec(std::map<int, String> & sortedStrings, const String & prefix) const;
    };

    class Trie
    {
    public:
        Trie() = default;
        Trie(bool hasWord_) : has_word(hasWord_) { }
        void insert(String key, int rowNumber);
        bool search(String);
        void showChildrenKeys();
        RadixTrie createRadixTree(String suffix = "") const;

        int countNodes();
        int countRows();
        std::set<String> getDistinctKeys();

        bool has_word = false;
        std::map<const String, Trie> children;
        std::vector<int> rows;
    };

    class Serde
    {
    public:
        union CharInteger
        {
            unsigned int integer;
            unsigned char byte[4];
        };
        String encodeTree(RadixTrie & node);
        std::tuple<RadixTrie, int> decodeTree(const std::string & encodedTree, int offset) const;
        String encodeSingleNode(const RadixTrie & node);

        String serialize(RadixTrie & node);
        RadixTrie deserialize(const String & encodedObj);
        std::pair<unsigned int, int> deserializeIntDynamically(const String & serializedObj, int offsetInString) const;
        static void serializeIntDynamically(unsigned int i, String & serializedObj);

    private:
        using StringToInt = std::map<String, int>;
        using IntToString = std::map<int, String>;

        int string_number;

        StringToInt string_to_int;
        IntToString int_to_string;

        static void serializeIntInString(unsigned int i, std::string & serializedObj, int nbBytesPerInteger);
        static int intGetNbBytesPerInteger(Int64 maxValue);
        static unsigned int deSerializeIntFromString(const std::string & serializedObj, int nbBytesPerInteger, int offsetInString);
        static String encodeDictionary(std::map<String, int> dict, IntToString reverseDict);
        static std::pair<IntToString, int> decodeDictionary(String serializedObj);
    };

    std::unique_ptr<Serde> serde_ptr;
private:
    Poco::Logger * log;
};

class CompressionCodecFactory;
void registerCodecTrieString(CompressionCodecFactory & factory);
};
