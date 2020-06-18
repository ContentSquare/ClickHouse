#include <Compression/CompressionCodecTrieString.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/hex.h>

namespace DB
{
UInt32 CompressionCodecTrieString::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    ReadBufferFromMemory istr(source, source_size);
    std::vector<String> keys;
    this->deserializeKeysFromBlockString(istr, keys);
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

    LOG_DEBUG(log, std::string("Number of nodes of radix after compressing: ") + std::to_string(radix_tree.countNodes()));
    LOG_DEBUG(log, std::string("Number of rows after compressing: ") + std::to_string(radix_tree.countRows()));
    memcpy(dest, serialized_radix_tree.data(), serialized_radix_size);
    return serialized_radix_size;
}

void CompressionCodecTrieString::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    // todo this gotta be improved
    auto raw_data = String(source, source_size);

    auto radix_tree = serde_ptr->deserialize(raw_data);
    std::vector<String> lines;
    radix_tree.generateLines(lines);

    LOG_DEBUG(log, std::string("Number of nodes of radix after decompressing: ") + std::to_string(radix_tree.countNodes()));
    LOG_DEBUG(log, std::string("Number of rows after decompressing: ") + std::to_string(radix_tree.countRows()));

    // we have to encode vector of found lines into buffer of characters
    WriteBuffer writeBuffer(&dest[0], uncompressed_size);

    for (const auto & line : lines)
    {
        auto size = static_cast<UInt64>(line.size());
        writeVarUInt(size, writeBuffer);
        writeString(line.data(), writeBuffer);
    }
}

String CompressionCodecTrieString::getCodecDesc() const
{
    return "TrieString";
}
uint8_t CompressionCodecTrieString::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::TRIE_String);
}
void CompressionCodecTrieString::deserializeKeysFromBlockString(
    ReadBuffer & istr, CompressionCodecTrieString::KeysInBlock & keysInBlock) const
{
    UInt64 size;
    String s = std::string{};
    while (!istr.eof())
    {
        s.clear();
        readVarUInt(size, istr);
        s.resize(size);
        istr.readStrict(s.data(), size);
        keysInBlock.push_back(s);
    }
}

void CompressionCodecTrieString::printFoundKeys(CompressionCodecTrieString::KeysInBlock & keys) const
{
    auto str = std::string("Found key is: ");
    LOG_DEBUG(log, std::string("Number of found keys: ") + std::to_string(keys.size()));

    for (const auto & key : keys)
    {
        LOG_DEBUG(log, str + key);
    }
}

void registerCodecTrieString(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::TRIE_String);
    factory.registerSimpleCompressionCodec("TrieString", method_code, [&]() { return std::make_shared<CompressionCodecTrieString>(); });
}

static bool isPrefix(std::string prefix, std::string toCheck)
{
    return std::mismatch(prefix.begin(), prefix.end(), toCheck.begin()).first == prefix.end();
}

bool CompressionCodecTrieString::RadixTrie::radixSearch(const String & key)
{
    bool result = false;
    RadixTrie * curr = this;
    for (RadixTrie & child : curr->children)
    {
        String child_key = child.radix_treesuffix;
        if (child_key == key)
        {
            return child.has_word;
        }
        else if (isPrefix(child_key, key))
        {
            auto reduced_key = key.substr(child_key.size(), key.size());
            result |= child.radixSearch(reduced_key);
        }
    }
    return result;
}


void CompressionCodecTrieString::RadixTrie::showChildrenKeys()
{
    std::cout << "listing all keys of node" << std::endl << std::endl;

    for (RadixTrie & child : children)
    {
        std::cout << child.radix_treesuffix << std::endl;
    }
}

int CompressionCodecTrieString::RadixTrie::countNodes()
{
    int count = 1;
    for (RadixTrie & child : children)
    {
        count += child.countNodes();
    }
    return count;
}

int CompressionCodecTrieString::RadixTrie::countRows()
{
    int count = this->rows.size();
    for (RadixTrie & child : this->children)
    {
        count += child.countRows();
    }
    return count;
}

std::set<std::string> CompressionCodecTrieString::RadixTrie::getDistinctKeys()
{
    std::set<std::string> result;
    for (auto & child : this->children)
    {
        auto child_result = child.getDistinctKeys();
        result.insert(child_result.begin(), child_result.end());
        result.insert(child.radix_treesuffix);
    }
    return result;
}

void CompressionCodecTrieString::RadixTrie::generateLines(std::vector<String> & lines)
{
    std::map<int, std::string> sorted_strings;
    generateLinesRec(sorted_strings, this->radix_treesuffix);
    lines.reserve(sorted_strings.size());
    for (const auto & pair : sorted_strings)
    {
        lines.push_back(pair.second);
    }
}

void CompressionCodecTrieString::RadixTrie::generateLinesRec(std::map<int, String> & sortedStrings, const String & prefix) const
{
    for (const auto & id : this->rows)
    {
        sortedStrings[id] = prefix;
    }
    for (const RadixTrie & child : this->children)
    {
        child.generateLinesRec(sortedStrings, prefix + child.radix_treesuffix);
    }
}

// Iterative function to insert a key in the Trie
void CompressionCodecTrieString::Trie::insert(String key, int rowNumber)
{
    // start from root node
    Trie * curr = this;
    for (char i : key)
    {
        String s_key = std::string(1, i);
        auto it = curr->children.find(s_key);
        if (it == curr->children.end())
        {
            curr->children[s_key] = Trie();
            curr = &curr->children[s_key];
        }
        else
        {
            curr = &it->second;
        }
    }
    curr->rows.push_back(rowNumber);
    curr->has_word = true;
}

bool CompressionCodecTrieString::Trie::search(String key)
{
    //    if (this == nullptr)
    //        return false;

    Trie * curr = this;
    for (char i : key)
    {
        String s_key = std::string(1, i);
        auto it = curr->children.find(s_key);
        if (it == curr->children.end())
        {
            return false;
        }
        curr = &curr->children[s_key];
    }

    return curr->has_word;
}

CompressionCodecTrieString::RadixTrie CompressionCodecTrieString::Trie::createRadixTree(std::string suffix) const
{
    if (this->has_word || this->children.size() > 1)
    {
        RadixTrie current(suffix, this->has_word, this->rows);
        for (const auto & pair : this->children)
        {
            auto child_key = pair.first;
            const Trie & child = pair.second;
            current.children.push_back(child.createRadixTree(child_key));
        }
        return current;
    }
    else if (this->children.size() == 1)
    {
        // since this child adds no value, we remove it
        // from the tree
        const Trie & child = (*this->children.begin()).second;
        auto child_key = (*this->children.begin()).first;
        return child.createRadixTree(suffix + child_key);
    }
    else
    {
        std::cerr << "can't have a node without a word and without children";
        abort();
    }
}

void CompressionCodecTrieString::Trie::showChildrenKeys()
{
    std::cout << "listing all keys of node" << std::endl << std::endl;

    for (const auto & kv : children)
    {
        std::cout << kv.first << std::endl;
    }
}

int CompressionCodecTrieString::Trie::countNodes()
{
    int count = 1;
    for (auto & pair : this->children)
    {
        count += pair.second.countNodes();
    }
    return count;
}
int CompressionCodecTrieString::Trie::countRows()
{
    int count = this->rows.size();
    for (auto & pair : this->children)
    {
        count += pair.second.countRows();
    }
    return count;
}

std::set<std::string> CompressionCodecTrieString::Trie::getDistinctKeys()
{
    std::set<std::string> result;
    for (auto & pair : this->children)
    {
        auto child_result = pair.second.getDistinctKeys();
        result.insert(child_result.begin(), child_result.end());
        result.insert(pair.first);
    }
    return result;
}

void CompressionCodecTrieString::Serde::serializeIntInString(unsigned int i, std::string & serializedObj, int nbBytesPerInteger)
{
    CharInteger id;
    id.integer = i;
    if (nbBytesPerInteger >= 1)
    {
        serializedObj.push_back(id.byte[0]);
    }
    if (nbBytesPerInteger >= 2)
    {
        serializedObj.push_back(id.byte[1]);
    }
    if (nbBytesPerInteger >= 3)
    {
        serializedObj.push_back(id.byte[2]);
    }
    if (nbBytesPerInteger >= 4)
    {
        serializedObj.push_back(id.byte[3]);
    }
}


int CompressionCodecTrieString::Serde::intGetNbBytesPerInteger(Int64 maxValue)
{
    if (maxValue < 0)
    {
        std::cout << "wrong value, should be >=0, value: " << maxValue << std::endl;
        abort();
    }
    else if (maxValue < (1l << 8))
    {
        return 1;
    }
    else if (maxValue < (1l << 16))
    {
        return 2;
    }
    else if (maxValue < (1l << 24))
    {
        return 3;
    }
    else if (maxValue < (1l << 32))
    {
        return 4;
    }
    else
    {
        std::cout << "wrong value, should be <2^32, value: " << maxValue << std::endl;
        abort();
    }
}

// can automatically decide the minimalvnumber of bytes this integer need to have for encoding.
// it only works for integer < 2^30
void CompressionCodecTrieString::Serde::serializeIntDynamically(unsigned int i, std::string & serializedObj)
{
    CharInteger id;
    id.integer = i;

    if (i >= static_cast<unsigned int>(2 << 30))
    {
        std::cout << "the given integer is above 2^30";
        abort();
    }
    // since the integer is <2^30, we can multiple is by 2^2 in order to use the 2²2 bits
    // to encdoe the number of bytes required
    i = i << 2;
    int nb_bytes_to_encode = intGetNbBytesPerInteger(i);
    i += nb_bytes_to_encode - 1; // nbBytesToEncode goes from 1 to 4, we need to put it in 2 bitss
    serializeIntInString(i, serializedObj, nb_bytes_to_encode);
}

unsigned int
CompressionCodecTrieString::Serde::deSerializeIntFromString(const std::string & serializedObj, int nbBytesPerInteger, int offsetInString)
{
    CharInteger id;
    id.integer = 0;
    if (nbBytesPerInteger >= 1)
    {
        id.byte[0] = serializedObj.at(offsetInString);
    }
    if (nbBytesPerInteger >= 2)
    {
        id.byte[1] = serializedObj.at(1 + offsetInString);
    }
    if (nbBytesPerInteger >= 3)
    {
        id.byte[2] = serializedObj.at(2 + offsetInString);
    }
    if (nbBytesPerInteger >= 4)
    {
        id.byte[3] = serializedObj.at(3 + offsetInString);
    }
    return id.integer;
}
// returns an integer serialized by the funcction serializeIntDynamically()
std::pair<unsigned int, int>
CompressionCodecTrieString::Serde::deserializeIntDynamically(const std::string & serializedObj, int offsetInString) const
{
    uint nb_bytes_per_integer = (serializedObj.at(offsetInString) & 3) + 1;
    uint deserialized_int = deSerializeIntFromString(serializedObj, nb_bytes_per_integer, offsetInString) / 4;
    return std::make_pair(deserialized_int, nb_bytes_per_integer);
}

String CompressionCodecTrieString::Serde::encodeDictionary(const StringToInt dict, const IntToString reverseDict)
{
    std::string serialized_dict;
    int size = dict.size();
    serializeIntInString(size, serialized_dict, 4);

    // the map contains values that are contiguous integers
    // from 1 to {size}
    // so we don't need to serialize the value
    // if we send the keys in the right order
    // using the reserve dict that is sorted by the integers
    for (const auto & pair : reverseDict)
    {
        serialized_dict.append(pair.second);
        serialized_dict.push_back('\0');
    }
    return serialized_dict;
}

std::pair<std::map<int, String>, int> CompressionCodecTrieString::Serde::decodeDictionary(const String serializedObj)
{
    int size = deSerializeIntFromString(serializedObj, 4, 0);
    int offset = 4;
    int nb_string_deser = 0;
    std::map<int, std::string> dict;
    String tmp_str;
    while (nb_string_deser < size)
    {
        const char cur = serializedObj.at(offset++);
        if (cur == '\0')
        {
            dict[++nb_string_deser] = tmp_str;
            tmp_str.clear();
        }
        else
        {
            tmp_str.push_back(cur);
        }
    }
    return std::make_pair(dict, offset);
}


String CompressionCodecTrieString::Serde::encodeTree(CompressionCodecTrieString::RadixTrie & node)
{
    String current_node_serialised = encodeSingleNode(node);
    for (RadixTrie & child : node.children)
    {
        String child_serialized = encodeTree(child);
        current_node_serialised.append(child_serialized);
    }
    return current_node_serialised;
}

String CompressionCodecTrieString::Serde::serialize(RadixTrie & node)
{
    string_to_int.clear();
    int_to_string.clear();
    string_number = 0;
    auto trie_encoded = encodeTree(node);
    auto dict_encoded = encodeDictionary(string_to_int, int_to_string);

    return dict_encoded + trie_encoded;
}

static int callNb2 = 0;

String CompressionCodecTrieString::Serde::encodeSingleNode(const RadixTrie & node)
{
    std::string serialized_node;
    serializeIntDynamically(node.children.size(), serialized_node);
    int string_id = string_to_int[node.radix_treesuffix];
    if (string_id == 0)
    {
        string_id = ++string_number;
        string_to_int[node.radix_treesuffix] = string_id;
        int_to_string[string_id] = node.radix_treesuffix;
    }
    serializeIntDynamically(string_id, serialized_node);
    // for the rows matching a word, we put the real rowNumber off the first
    // one, then the other are just the difference between the previous one (delta-encoding)
    // in order to reduce the bytes needed to store the rowsNumbers.
    // because of the way the data are stored, we have the garantee that the difference between
    // 2 consecutive rowsNumber will always be positive
    int previous_row_number = -1;
    int previous_delta = -1;
    int number_of_delta_repetitions = 0;
    for (int row_number : node.rows)
    {
        if (previous_row_number == -1)
        {
            serializeIntDynamically(row_number, serialized_node);
        }
        else
        {
            int delta = row_number - previous_row_number;
            //before serializing the deltas, we check if there is a repeting
            //sequences of the same deltas so that we can apply RLE
            // the charactere "activate" RLE is 0 because it's not possible
            // for deltas to be <1.
            // the format in case of RLE is [O][the number to repet][the number of repetitions]
            if (previous_delta == delta)
            {
                number_of_delta_repetitions++;
            }
            else
            {
                //if the numberOfDeltaRepetitions<=2
                // the overhead of the RLE format is bigger
                // than writings the delta(s)
                if (number_of_delta_repetitions > 2)
                {
                    serializeIntDynamically(RLE_ACTIVATION_FLAG, serialized_node);
                    serializeIntDynamically(previous_delta, serialized_node);
                    serializeIntDynamically(number_of_delta_repetitions, serialized_node);
                }
                else
                {
                    for (int i = 0; i < number_of_delta_repetitions; i++)
                    {
                        serializeIntDynamically(previous_delta, serialized_node);
                    }
                }

                previous_delta = delta;
                number_of_delta_repetitions = 1;
            }
        }
        previous_row_number = row_number;
    }

    if (number_of_delta_repetitions > 2)
    {
        serializeIntDynamically(RLE_ACTIVATION_FLAG, serialized_node);
        serializeIntDynamically(previous_delta, serialized_node);
        serializeIntDynamically(number_of_delta_repetitions, serialized_node);
    }
    else
    {
        for (int i = 0; i < number_of_delta_repetitions; i++)
        {
            serializeIntDynamically(previous_delta, serialized_node);
        }
    }

    int size_node_in_byte = serialized_node.size();
    String serialized_size;
    serializeIntDynamically(size_node_in_byte, serialized_size);
    if (false && callNb2 == 97)
    {
        std::cout << "======================" << std::endl;
        std::cout << "callNb =" << callNb2 << std::endl;
        std::cout << "sizeInBytes =" << size_node_in_byte << std::endl;
        std::cout << "nbChildren =" << node.children.size() << std::endl;
        std::cout << "strinKey =" << string_to_int[node.radix_treesuffix] << std::endl;
        std::cout << "nbRows =" << node.rows.size() << std::endl;
        for (auto row : node.rows)
        {
            std::cout << row << "||";
        }
        std::cout << std::endl;
        std::cout << std::endl << "offset =" << 0 << std::endl;
    }
    return serialized_size + serialized_node;
}
std::tuple<CompressionCodecTrieString::RadixTrie, int>
CompressionCodecTrieString::Serde::decodeTree(const String & encodedTree, int offset) const
{
    auto raw_size = deserializeIntDynamically(encodedTree, offset);
    int size_in_bytes = raw_size.first;
    int size_of_size_field = raw_size.second;
    int cur_offset = offset + size_of_size_field;
    auto raw_nb_children = deserializeIntDynamically(encodedTree, cur_offset);
    int nb_children = raw_nb_children.first;
    cur_offset += raw_nb_children.second;
    auto raw_string_key = deserializeIntDynamically(encodedTree, cur_offset);
    int string_key = raw_string_key.first;
    cur_offset += raw_string_key.second;
    std::vector<int> rows;
    int offset_next_node = offset + size_in_bytes + size_of_size_field;
    int rows_offset = cur_offset;
    // for the rows matching a word, we put the real rowNumber off the first
    // one, then the other are just the difference between the previous one (delta-encoding)
    // in order to reduce the bytes needed to store the rowsNumbers.
    // because of the way the data are stored, we have the garantee that the difference between
    // 2 consecutive rowsNumber will always be positive
    int previous_row = -1;
    while (rows_offset < offset_next_node)
    {
        auto raw_row = deserializeIntDynamically(encodedTree, rows_offset);
        rows_offset += raw_row.second;
        int value = raw_row.first;
        if (previous_row == -1)
        {
            previous_row = value;
            rows.push_back(previous_row);
        }
        else
        {
            //before deserializing the deltas, we need check if it was encoded with RLE
            // the format in case of RLE is [O][the number to repet][the number of repetitions]
            if (value != RLE_ACTIVATION_FLAG)
            {
                previous_row += value;
                rows.push_back(previous_row);
            }
            else
            {
                auto integer_to_repeat = deserializeIntDynamically(encodedTree, rows_offset);
                rows_offset += integer_to_repeat.second;
                auto nb_repetition = deserializeIntDynamically(encodedTree, rows_offset);
                rows_offset += nb_repetition.second;

                for (unsigned int i = 0; i < nb_repetition.first; i++)
                {
                    previous_row += integer_to_repeat.first;
                    rows.push_back(previous_row);
                }
            }
        }
    }
    String key = int_to_string.at(string_key);
    RadixTrie tree(key, !rows.empty(), rows);
    if (size_in_bytes == 0)
        abort();
    for (int i = 0; i < nb_children; i++)
    {
        std::tuple<RadixTrie, int> child_decoded_node = decodeTree(encodedTree, offset_next_node);
        RadixTrie child = std::get<0>(child_decoded_node);
        auto size_node_child = std::get<1>(child_decoded_node);
        tree.children.push_back(child);
        offset_next_node = size_node_child;
    }
    return std::make_tuple(tree, offset_next_node);
}


CompressionCodecTrieString::RadixTrie CompressionCodecTrieString::Serde::deserialize(const std::string & encodedObj)
{
    string_to_int.clear();
    int_to_string.clear();
    std::pair<std::map<int, String>, int> pair = decodeDictionary(encodedObj);
    int_to_string = pair.first;
    int current_offset = pair.second;
    std::tuple<RadixTrie, int> decoded_node = decodeTree(encodedObj, current_offset);
    return std::get<0>(decoded_node);
}

}
