# Top level makefile, the real shit is at src/Makefile

default: all

# 如果执行make xx，而xx没有匹配到时就执行.DEFAULT下语句，$@是输入的参数
.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

# 伪目标，始终被认为“需要执行”的目标
# 即使当前目录下存在名为 install 的文件或目录，也会正常执行install下的命令
.PHONY: install
