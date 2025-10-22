

namespace Contest {

template <typename Key, typename Value>
class GenHash {
public:
    virtual ~GenHash() = default;
    virtual void emplace(const Key& key,const Value& value) = 0;
    virtual bool contains(const Key& key) = 0;
    virtual Value& operator[](const Key& key) = 0;
};

}