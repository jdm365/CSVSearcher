#include <string_view>
#include <cstring>

#define SMALL_STRING_SIZE 32

struct SmallString {
    union {
        char  short_key[SMALL_STRING_SIZE];
        char* long_key;
    };
    bool is_long;

    SmallString(const char* str) {
        auto len = std::strlen(str);
        if (len <= SMALL_STRING_SIZE) {
            std::strcpy(short_key, str);
            is_long = false;
        } else {
            long_key = new char[len + 1];
            std::strcpy(long_key, str);
            is_long = true;
        }
    }

    ~SmallString() {
        if (is_long) {
            delete[] long_key;
        }
    }

	SmallString(const SmallString& other) {
		if (other.is_long) {
			long_key = new char[std::strlen(other.long_key) + 1];
			std::strcpy(long_key, other.long_key);
		} else {
			std::strcpy(short_key, other.short_key);
		}
		is_long = other.is_long;
	}

	SmallString& operator=(const SmallString& other) {
		if (this != &other) {
			if (is_long) {
				delete[] long_key;
			}
			if (other.is_long) {
				long_key = new char[std::strlen(other.long_key) + 1];
				std::strcpy(long_key, other.long_key);
			} else {
				std::strcpy(short_key, other.short_key);
			}
			is_long = other.is_long;
		}
		return *this;
	}

    bool operator==(const SmallString& other) const {
        if (is_long != other.is_long) {
            return false;
        }
        if (is_long) {
            return std::strcmp(long_key, other.long_key) == 0;
        } else {
            return std::strcmp(short_key, other.short_key) == 0;
        }
    }
};

template <>
struct std::hash<SmallString> {
    std::size_t operator()(const SmallString& k) const {
        if (k.is_long) {
            return std::hash<std::string_view>()(std::string_view(k.long_key));
        } else {
            return std::hash<std::string_view>()(std::string_view(k.short_key));
        }
    }
};
