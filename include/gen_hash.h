

namespace Contest {
    
template <typename Key, typename Value>
class GenHash {
    virtual ~GenHash() = default;
    virtual void emplace(const Key& key,const std::vector<Value>& values) = 0;
    virtual bool contains(const Key& key) = 0;
    virtual std::vector<Value>& operator[](const Key& key) = 0;
};

}