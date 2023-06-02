func(){
	cd $1
    for file in `ls .`
    do
        if test -f $file
        then
            echo "removed `pwd`/"$file
	    rm -rf $file
        elif test -d $file
        then
            echo "into path: `pwd`/"$file
            func $file
        fi
    done
    cd ..
}
 
path="bin"
func $path
path="folder_for_test"
func $path


rm -rf build
echo "removed `pwd`/build"
rm -rf lib/*
echo "removed `pwd`/lib/*"