#ifndef UTILS_H
#define UTILS_H
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <sstream>
#include <vector>

namespace utils {
/**
 * @brief 检查目录是否存在
 * @param[in] path 检查的目录
 * @return true path存在
 * @return false path不存在
 */
static inline bool DirExists(std::string path) {
  struct stat st;
  int ret = stat(path.c_str(), &st);
  return ret == 0 && st.st_mode & S_IFDIR;
}

/**
 * @brief 列出一个目录中的所有文件名字
 * @details 目录路径
 * @param[in] path 目录路径
 * @param[in] ret 该目录下的所有文件名字
 * @return int 文件数量
 */
static inline int ScanDir(std::string path, std::vector<std::string> &ret) {
  DIR *dir;
  struct dirent *rent;
  dir = opendir(path.c_str());
  char s[100];
  while ((rent = readdir(dir))) {
    strcpy(s, rent->d_name);
    if (s[0] != '.') {
      ret.push_back(s);
    }
  }
  closedir(dir);
  return ret.size();
}

/**
 * @brief 创建目录
 * @param[in] path 要创建的目录路径
 * @return int 成功返回0，失败返回-1
 */
static inline int _Mkdir(const char *path) { return ::mkdir(path, 0775); }

/**
 * @brief 递归地创建目录
 * @param[in] path 创建的目录路径
 * @return int 成功返回0，失败返回-1
 */
static inline int Mkdir(const char *path) {
  std::string current_path = "";
  std::string dir_name;
  std::stringstream ss(path);

  while (std::getline(ss, dir_name, '/')) {
    current_path += dir_name;
    if (!DirExists(current_path) && _Mkdir(current_path.c_str()) != 0) {
      return -1;
    }
    current_path += "/";
  }
  return 0;
}

/**
 * @brief 删除一个空目录
 * @param[in] path 要删除的空目录路径
 * @return int 成功返回0，失败返回-1
 */
static inline int RmDir(const char *path) { return ::rmdir(path); }

/**
 * @brief 删除一个文件
 * @param[in] path 要删除文件的路径
 * @return int 成功返回0，失败返回-1
 */
static inline int RmFile(const char *path) { return ::unlink(path); }

}  // namespace utils

#endif