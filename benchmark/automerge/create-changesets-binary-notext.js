/*
 * This script executes the paper editing trace, one char at a time
 * This version does not use Automerge.Text
 * 
 * */
const { edits, finalText } = require('./editing-trace')
const Automerge = require('automerge')
const fs = require('fs');
const { randomUUID } = require('crypto');
var args = process.argv.slice(2);

// The batch size (interval) is given as command line parameter
let interval = Number(args[0])

// Save the starting time
const start = new Date()

// Initialize the state
let state = Automerge.from({text: []})

// Reference to the new state
let newstate = null

// Changeset directory (created if it does not exist)
var dir = "./automerge-binary-notext-"+args[0];
if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, 0744);
}

// Get and store the initial changeset
var changes = Automerge.getAllChanges(state)
fs.writeFileSync(dir+'/aaa-iter-0.json',  new Buffer(changes[0]), null);

// Apply the editing trace
for (let i = 0; i < edits.length;) {
  // Compute the new state
  const batchChangeStart = new Date()
  newstate = Automerge.change(state, doc => {
	for (let j=0; i <edits.length && j < interval; i++, j++) {
		if (edits[i][1] > 0) doc.text.deleteAt(edits[i][0], edits[i][1])
		else if (edits[i].length > 2) {
			let t = edits[i][2]
			let thechar = { "_id" : randomUUID(), "#" :  t.charCodeAt(0).toString(16) }
			doc.text.insertAt(edits[i][0], thechar)
		}
	}
  })
  const batchChangeEnd = new Date()
  // Determine the changeset between the current state and the previous state
  let changes = Automerge.getChanges(state, newstate)
  const batchGetChangesEnd = new Date()
  // Store the changeset on disk
  fs.writeFileSync(dir+'/iter-'+String(i).padStart(7, '0')+'.json',  new Buffer(changes[0]), null);
  const batchCommitEnd = new Date()
  // Update the reference to the current state
  state = newstate
  const batchStateRefUpdate = new Date()
  totalTime = new Date() - start
  var eps = i / (totalTime / 1000.0) 
  const rss = process.memoryUsage().rss
  console.log(`${i},edits,${totalTime},total_ms,${eps},eps,${batchChangeEnd-batchChangeStart},change_ms,${batchGetChangesEnd-batchChangeEnd},getChanges_ms,${batchCommitEnd-batchGetChangesEnd},writeFileSync_ms,${batchStateRefUpdate-batchCommitEnd},stateRefUpdate_ms,${rss},rss`)
}

if (state.text.join('') !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
