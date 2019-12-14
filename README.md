# 1
Create two folders in the cs-425-mp4 root directory titled
- clientFiles
- serverFiles

 Put any files you want to upload inside of clientFiles

# 2
Start up all the servers of the sdfs using
- go run serverMain.go

The server also has a few commands
- id (Prints out the hostname of the server)
- list (Prints out the membership list)
- store (Prints out all the file names stored at the server)
- leave (The server will leave the network)

# 3
Use the client to upload input files and execitables to sdfs
- go run clientMain.go clientMain.go put localFileName sdfsFileName
    -  "localFileName" is the local filename you want to upload to the sdfs and "sdfsFileName" is the name that you want for the file to have within the sdfs

- go run clientMain.go ls sdfsFileName
	- The client will print out the hostnames of all servers that store the file "sdfsFileName"

NOTE - When making client requests, do not include clientFiles/ in the name of the local file

# 4
Use the client to submit maple and juice jobs to master

- go run clientMain.go maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
- go run clientMain.go juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}