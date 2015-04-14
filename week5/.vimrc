set autoindent    "自动缩进 
set shiftwidth=2  "设置缩进为2
set number        "显示行号
set cindent       "c语言缩进
syntax on	  "语法高亮
set showmatch     "括号高亮匹配
set smartindent   "智能缩进，也不知道干嘛的
set nobackup      "不要备份
set noswapfile    "不要缓存文件
filetype on	  "检测文件类型
set tabstop=2     "设置tab为2
"针对不同的文件采用不同的缩进格式
filetype indent on
"允许插件
filetype plugin on
"启动自动补全
filetype plugin indent on

"按F9时，启用c++编译并运行
map <F9> :call CompileRunGpp()<CR>
func! CompileRunGpp()
exec "w"
exec "!g++ % -o %<"
exec "! ./%<"
endfunc
