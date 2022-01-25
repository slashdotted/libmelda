/*
 * This script applies a collection of changesets (as generated by create-changesets-binary.js)
 * This version uses Automerge.Text
 * 
 * */
const { finalText } = require('./editing-trace')
const fs = require('fs');
const Automerge = require('automerge')
var args = process.argv.slice(2);
// The batch size (interval) is given as command line parameter
var dir = "./automerge-binary-"+args[0]

// Initialize the state
let doc = Automerge.init()

// Save the starting time
const start = new Date()

// Read the changeset directory
fs.readdir(dir, (err, files) => {
  // For each changeset, apply the changes
  files.forEach(file => {
      const startReadFile = new Date()
	  const changes = fs.readFileSync(dir+"/"+file, null );
	  const startApplyChanges = new Date()
	  let [newdoc, patch] = Automerge.applyChanges(doc, [changes])
	  const endApplyChanges = new Date()
      doc = newdoc
      const endUpdateDocRef = new Date()
      const rss = process.memoryUsage().rss
      console.log(`${file},filename,${new Date() - start},total_ms,${startApplyChanges-startReadFile},readFileSync_ms,${endApplyChanges-startApplyChanges},applyChanges_ms,${endUpdateDocRef-endApplyChanges},stateRefUpdate_ms,${rss},rss_bytes`)
  });
  const readstart = new Date()
  var txt = doc.text.toString()
  console.error(txt)
  console.log(`Read time ${new Date() - readstart} ms`)
  if (txt !== finalText) {
      throw new RangeError('ERROR: final text did not match expectation' 
  }
});





