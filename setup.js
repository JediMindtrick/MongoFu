var doc = "Sorry I'm not doing this automatically yet...\n" +
'But before  you get started, please do the following:\n' +
'Download the mongodb binaries and extract them to [ThisDirectory]/mongodb.\n';
'Then, go to [ThisDirectory]/mongodb/mongodb.conf\n' +
'And change the following two lines to something that makes sense for you\n' +
'dbpath=<your project directory>/mongodb/data/db\n' +
'logpath=<your project directory>/mongodb/data/mongodb.log\n' +
'Then copy the file into <your project directory>/mongodb/data .';

console.log(doc);