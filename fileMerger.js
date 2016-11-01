"use strict";

const
  fs = require('fs'),
  crypto = require('crypto'),
  noop = function(){},
  indir = "./in/",
  outdir = "./out/",
  rejectdir = "./reject/";

var
  hashes = {},
  rejects = {},
  datasets = fs.readdirSync(indir);

console.log("Reading files from these directories:");
console.log(datasets);

hashExisting(nextDataset);

function nextDataset() {
  var dataset = datasets.pop();
  
  if (dataset) {
    readDataset(dataset, nextDataset);
  }
}

//get a list of already existing files and thier hashes
function hashExisting(cb) {
  const files = fs.readdirSync(outdir);
  let processing = 0;
  
  cb = typeof cb === "function" ? cb : noop;
  
  if (files.length) {
    console.log(
      "Calculating hashes of the " + files.length +
      " file(s) that already exist in the 'out/' directory"
    );
  } else {
    console.log("'out/' directory is empty, moving on to new files.");
    cb();
    return;
  }
  
  for (let file of files) {
    let
      fd = fs.createReadStream(outdir+file),
      hash = crypto.createHash('md5').setEncoding('hex');

    processing++;

    fd.on('end', function() {
      hash.end();
      hashes[file] = hash.read();

      processing--;
      fileDone(null, processing, cb);
    });

    // read all file and pipe it to the hash object
    fd.pipe(hash);
  }

  processing--;
  fileDone(null, processing, cb);
}

//process input files
function readDataset(dataset, cb) {
  console.log("Starting dataset: " + dataset);

  const files = fs.readdirSync(indir + dataset + "/");

  console.log("Contains " + files.length + " files.");
  
  cb = typeof cb === "function" ? cb : noop;

  processFile(indir + dataset + "/", files.pop(), nextFile);
  function nextFile() {
    if (files.length > 0) {
      processFile(indir + dataset + "/", files.pop(), nextFile);
    } else {
      cb();
    }
  }
}

function processFile(datapath, file, cb) {
  let processing = 1;

  console.log("Reading " + datapath + file);

  //calculate the file's hash
  let hash = crypto.createHash('md5').setEncoding('hex');
  fs.createReadStream(datapath+file).on('end', function() {
    hash.end();
    let digest = hash.read();

    //check if the already exists in the out directory
    if (!hashes[file]) {
      //file does not exist, copy it over
      processing++;
      copyFile(datapath+file, outdir+file, function(err) {
        processing--;
        fileDone(err, processing, cb);
      });
      hashes[file] = digest;
    } else {
      if (hashes[file] !== digest) {
        //the file already exists, but its has doesnt match the existing file
        
        //keep track of how many times each hash has been seen
        if (!rejects[file]) {rejects[file] = {};}
        rejects[file][digest] =
          !!rejects[file][digest] ? rejects[file][digest]++ : 1; 
        rejects[file][hashes[file]] =
          !!rejects[file][hashes[file]] ? rejects[file][hashes[file]]++ : 1;
        
        //copy both to the reject directory.
        processing++;
        copyFile(datapath+file, rejectdir+file+"."+hash.read, function(err) {
          processing--;
          fileDone(err, processing, cb);
        });
        processing++;
        copyFile(outdir+file, rejectdir+file+"."+hash.read, function(err) {
          processing--;
          fileDone(err, processing, cb);
        });

        //if the new file has been seen more times than the existing file,
        //copy it over and updated the hashes of existing files
        if (rejects[file][digest] > rejects[file][hashes[file]]) {
          hashes[file] = digest;
          processing++;
          copyFile(datapath+file, outdir+file, function(err) {
            processing--;
            fileDone(err, processing, cb);
          });
        }
      } else {
        //exact copy already exists, nothing to do
        processing--;
        fileDone(null, processing, cb);
      }
    }
  }).pipe(hash);

  processing--;
  fileDone(null, processing, cb);
}

function copyFile(source, target, cb) {
  var cbCalled = false;

  var rd = fs.createReadStream(source);
  rd.on("error", function(err) {
    done(err);
  });
  var wr = fs.createWriteStream(target);
  wr.on("error", function(err) {
    done(err);
  });
  wr.on("close", function(ex) {
    done();
  });
  rd.pipe(wr);

  function done(err) {
    if (!cbCalled) {
      cb(err);
      cbCalled = true;
    }
  }
}

//update the number of files that still need to be processed for this dataset
//call the callback if all have completed
function fileDone(err, processing, cb) {
  if (err) {
    console.error(err);
  }

  if (--processing) {
    cb(); 
  }
}