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
  } else {
    console.log("All datasets have been processed!");
  }
}

//get a list of already existing files and thier hashes
function hashExisting(cb) {
  const files = fs.readdirSync(outdir);
  let processingSteps = files.length;
  
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

    fd.on('end', function() {
      hash.end();
      hashes[file] = hash.read();

      processingSteps--;
      if (!processingSteps) {cb();};
    });

    // read all file and pipe it to the hash object
    fd.pipe(hash);
  }
}

//process input files
function readDataset(dataset, cb) {
  console.log("Starting dataset: " + dataset);

  const
    files = fs.readdirSync(indir + dataset + "/"),
    badFiles = files.filter(function(el){return !el;});
  
  console.log("Contains " + files.length + " files.");
  if (badFiles.length) {
    console.error("Found undefined file in " + dataset);
    console.error(badFiles);
    process.exit();
  }

  cb = typeof cb === "function" ? cb : noop;

  nextFile();
  function nextFile(err) {
    if (err) {
      console.log("Error processing file, exiting:");
      console.error(err);
      process.exit(1);
    }
    
    if (files.length) {
      let file = files.pop();
      console.log("Reading " + file + " from " + dataset + ".");
      setTimeout(processFile, 0, indir + dataset + "/", file, nextFile);
    } else {
      console.log("Finished dataset " + dataset + ".");
      cb();
    }
  }
}

function processFile(datapath, file, cb) {
  let processingSteps = 0;

  //calculate the file's hash
  let
    hash = crypto.createHash('md5').setEncoding('hex'),
    rd = fs.createReadStream(datapath+file);
  
  rd.on('end', function() {
    hash.end();
    rd.unpipe();
    let digest = hash.read();
    
    //check if the already exists in the out directory
    if (!hashes[file]) {
      //file does not exist, copy it over
      hashes[file] = digest;
      copyFile(datapath+file, outdir+file, function(err) {
        cb(err);
      });
    } else {
      if (hashes[file] !== digest) {
        //the file already exists, but its hash doesnt match the existing file

        //keep track of how many times each hash has been seen
        if (!rejects[file]) {rejects[file] = {};}
        rejects[file][digest] =
          !!rejects[file][digest] ? rejects[file][digest]++ : 1; 
        rejects[file][hashes[file]] =
          !!rejects[file][hashes[file]] ? rejects[file][hashes[file]]++ : 1;
        
        //if the new file has been seen more times than the existing file,
        //copy it over and updated the hashes of existing files
        if (rejects[file][digest] > rejects[file][hashes[file]]) {
          hashes[file] = digest;
          //we are copying 1 more file so we add one more processingStep
          processingSteps++;
          copyFile(datapath+file, outdir+file, function(err) {
            processingSteps--;
            if (!processingSteps || err) {cb(err);}
            if (processingSteps < 0) {
              console.error("Negative processing steps in replacing conflict.");
            }
          });
        }

        //copy both to the reject directory if this is the first encounter.
        if (rejects[file][digest] == 1) {
          processingSteps++;
          copyFile(datapath+file, rejectdir+file + "." + digest, function(err) {
            processingSteps--;
            if (!processingSteps || err) {cb(err);}
            if (processingSteps < 0) {
              console.error("Negative processing steps copying reject 1.");
            }
          });
        }
        if (rejects[file][hashes[file]] == 1) {
          processingSteps++;
          copyFile(outdir+file, rejectdir+file+"."+hashes[file], function(err) {
            processingSteps--;
            if (!processingSteps || err) {cb(err);}
            if (processingSteps < 0) {
              console.error("Negative processing steps copying reject 2.");
            }
          });
        }

        if (!processingSteps) {
          //The file has already been copied to the rejectdir in a previous
          //encounter. Move on to the next file
          cb(null);
        }
      } else {
        //exact copy already exists, nothing to do
        cb(null);
      }
    }
  }).on("error", function(err){
    console.error("Could not hash file " + datapath + " " + file + ":");
    cb(err || "File Error");
  }).pipe(hash);
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
    rd.close();
    wr.close();
    if (!cbCalled) {
      cb(err);
      cbCalled = true;
    }
  }
}