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

hashExisting(nextDataset);

function nextDataset() {
  var dataset = datasets.pop();
  
  if (dataset) {
    readDataset(nextDataset);
  }
}

//get a list of already existing files and thier hashes
function hashExisting(cb) {
  var processing = 0;
  const files = fs.readdirSync(outdir);

  cb = typeof cb === "function" ? cb : noop;
  
  for (let file of files) {
    let
      fd = fs.createReadStream(outdir+file),
      hash = crypto.createHash('md5').setEncoding('hex');

    processing++;

    fd.on('end', function() {
      hash.end();
      hashes[file] = hash.read();

      processing = fileDone(null, processing, cb);
    });

    // read all file and pipe it to the hash object
    fd.pipe(hash);
  }
}

//process input files
function readInputs(cb) {
  var processing = 0;
  const files = fs.readdirSync(indir);

  cb = typeof cb === "function" ? cb : noop;

  for (let file of files) {
    //check if the already exists in the out directory
    fs.access(outdir+file, function(err) {
      processing++;

      if (err) {
        //file does not exist, copy it over
        copyFile(indir+file, outdir+file, function(err) {
          processing = fileDone(err, processing, cb);
        });
      } else {
        //file already exists. check if its hash matches an existing file
        let
          fd = fs.createReadStream(outdir+file),
          hash = crypto.createHash('md5').setEncoding('hex');

        fd.on('end', function() {
          hash.end();
          var digest = hash.read();

          if (hashes[file] !== digest) {
            processing--;

            //the input file doenst match the existing file,
            //register both files as rejected
            if (!rejects[digest]) {
              rejects[digest] = 1;
            } else {
              rejects[digest]++;
            }
            if (!rejects[hashes[file]]) {
              rejects[hashes[file]] = 1;
            } else {
              rejects[hashes[file]]++;
            }

            //copy both to the reject directory.
            processing++;
            copyFile(indir+file, rejectdir+file+"."+hash.read, function(err) {
              processing = fileDone(err, processing, cb);
            });
            processing++;
            copyFile(outdir+file, rejectdir+file+"."+hash.read, function(err) {
              processing = fileDone(err, processing, cb);
            });

            //if the new file has been seen more times than the existing file,
            //copy it over
            if (rejects[digest] > rejects[hashes[file]]) {
              processing++;
              copyFile(indir+file, outdir+file, function(err) {
                processing = fileDone(err, processing, cb);
              });
            }
          } else {
            //exact copy already exists, nothing to do
            processing = fileDone(null, processing, cb);
          }
        });

        // read all file and pipe it to the hash object
        fd.pipe(hash);
      }
    });
  }
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

  return processing;
}