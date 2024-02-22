#include <cstdio>
#include <fstream>
#include <sstream>
#include <iostream>
#include <unordered_map>

using namespace std;

int main() {
  std::unordered_map<uint64_t, uint64_t> lba2content;
  std::unordered_map<uint64_t, uint64_t> lba2hash;
  const char* filename = "/mnt/data/debug_file/index_mapping.txt";
  // read the file line by line with fstream
  ifstream infile(filename);
  string line;  
  uint64_t lba, content, hash;
  uint64_t linenum = 0;
  while (getline(infile, line)) {
    // split the line into two parts
    istringstream iss(line);
    string key;
    iss >> key;

    if (key == "write") {
      // write lba [lba] content [content] hash [hash]
      string _k, _k2;
      iss >> _k >> _k2;
      if (_k != "lba") {
        printf("not lba %s\n", line.c_str());
        break;
      }
      lba = stoull(_k2);

      iss >> _k >> _k2;
      if (_k != "content") {
        printf("not content %s\n", line.c_str());
        break;
      }
      content = stoull(_k2); 

      iss >> _k >> _k2;
      if (_k != "hash") {
        printf("not hash %s\n", line.c_str());
        break;
      }
      hash = stoull(_k2);

      lba2content[lba] = content;
      lba2hash[lba] = hash;
    } else if (key == "finread") {
      // firead lba [lba] pba pba pba pba content [content] hash [hash]
      string _k, _k2;
      iss >> _k >> _k2;
      if (_k != "lba") {
        printf("not lba in read %s\n", line.c_str());
        break;
      }
      lba = stoull(_k2);

      iss >> _k >> _k >> _k >> _k >> _k >> _k2;  
      if (_k != "content") {
        printf("not content in read %s\n", line.c_str());
        break;
      }
      content = stoull(_k2);

      iss >> _k >> _k2;
      if (_k != "hash") {
        printf("not hash in read %s\n", line.c_str());
        break;
      }
      hash = stoull(_k2);

      // verify content and has
      if (lba2content[lba] != content) {
        printf("[line %lu] content not lba %lu\n", linenum, lba);
        break;
      }
      if (lba2hash[lba] != hash) {
        printf("[line %lu] hash not lba %lu\n", linenum, lba);
        break;
      }
    }
    linenum++;
  }
  printf("num lbas %lu\n", lba2content.size());
  printf("end line number %lu\n", linenum);
  return 0;
}
