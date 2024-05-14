#!/bin/bash

# 进入文件夹
cd job

# 遍历文件夹下的所有文件
for file in *.py; do
    # 检查文件是否是.py文件
    if [[ -f $file ]]; then
        # 运行脚本
        python "$file"
    fi
done