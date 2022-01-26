/*
 * This script executes the paper editing trace to an Automerge.Text object, one char at a time
 * and compares the results with the trace applied to an array of objects
 * 
 * */
const { edits, finalText } = require('./editing-trace')
const Automerge = require('automerge')
const fs = require('fs');
const { randomUUID } = require('crypto');

function compare_states(changeset,wtext,wotext) {
	// Generate text from Automerge.Text()
	var txt = wtext.text.toString()
	// Generate text from object array
	var arr = []
	wotext.text.forEach(c => {
		arr.push(parseInt(c["#"], 16))
	})
	var wotxt = String.fromCharCode(...arr)
	if (txt !== wotxt) {
      throw new RangeError(`ERROR: mismatch at ${changeset}`)
    } else {
	  console.log(`${changeset} matches`)
	}
}

function apply_and_compare(changeset) {
	// Read initial changeset
	const read_changes_with_text = fs.readFileSync(dir_with_text+changeset, null );
	const read_changes_without_text = fs.readFileSync(dir_without_text+changeset, null );
	// Apply initial changeset
	let [new_reread_doc_with_text, text_patch] = Automerge.applyChanges(reread_doc_with_text, [read_changes_with_text])
	let [new_reread_doc_without_text, notext_patch] = Automerge.applyChanges(reread_doc_without_text, [read_changes_without_text])
	// Update doc
	reread_doc_with_text = new_reread_doc_with_text
	reread_doc_without_text = new_reread_doc_without_text
	// Compare states
	compare_states(changeset,reread_doc_with_text,reread_doc_without_text)
}



var args = process.argv.slice(2);

// The batch size (interval) is given as command line parameter
let interval = Number(args[0])

// Initialize the states
let state_with_text = Automerge.from({text: new Automerge.Text()})
let state_without_text = Automerge.from({text: []})

let new_state_with_text = null
let new_state_without_text = null

// Changeset directory (created if it does not exist)
var dir_with_text = "./automerge-compare-text-"+args[0]
if (!fs.existsSync(dir_with_text)) {
    fs.mkdirSync(dir_with_text, 0744)
}
var dir_without_text = "./automerge-compare-notext-"+args[0]
if (!fs.existsSync(dir_without_text)) {
    fs.mkdirSync(dir_without_text, 0744)
}

// Get and store the initial changeset
var changes_with_text = Automerge.getAllChanges(state_with_text)
fs.writeFileSync(dir_with_text+'/aaa-iter-0.json',  new Buffer(changes_with_text[0]), null)

var changes_without_text = Automerge.getAllChanges(state_without_text)
fs.writeFileSync(dir_without_text+'/aaa-iter-0.json',  new Buffer(changes_without_text[0]), null)

// Initialize doc for comparison
var reread_doc_with_text = Automerge.init()
var reread_doc_without_text = Automerge.init()
apply_and_compare('/aaa-iter-0.json')

// Apply the editing trace
for (let i = 0; i < edits.length;) {
  // Update state_without_text
  starting_i = i // save i for later
  new_state_without_text = Automerge.change(state_without_text, doc => {
  for (let j=0; i <edits.length && j < interval; i++, j++) {
		if (edits[i][1] > 0) doc.text.deleteAt(edits[i][0], edits[i][1])
		else if (edits[i].length > 2) {
			let t = edits[i][2]
			let thechar = { "_id" : randomUUID(), "#" :  t.charCodeAt(0).toString(16) }
			doc.text.insertAt(edits[i][0], thechar)
		}
	}
  })
  // Update state_with_text
  i = starting_i // restore i
  new_state_with_text = Automerge.change(state_with_text, doc => {
  for (let j=0; i <edits.length && j < interval; i++, j++) {
		if (edits[i][1] > 0) doc.text.deleteAt(edits[i][0], edits[i][1])
		else if (edits[i].length > 2) doc.text.insertAt(edits[i][0], ...edits[i].slice(2))
	}
  })

  // Generate changes
  let changes_with_text = Automerge.getChanges(state_with_text, new_state_with_text)
  let changes_without_text = Automerge.getChanges(state_without_text, new_state_without_text)
  
  // Store the changeset on disk
  fs.writeFileSync(dir_with_text+'/iter-'+String(i).padStart(7, '0')+'.json',  new Buffer(changes_with_text[0]), null)
  fs.writeFileSync(dir_without_text+'/iter-'+String(i).padStart(7, '0')+'.json',  new Buffer(changes_without_text[0]), null)
  
  // Update the current state reference
  state_with_text = new_state_with_text
  state_without_text = new_state_without_text
  
  // Read and apply the changeset 
  apply_and_compare('/iter-'+String(i).padStart(7, '0')+'.json')  
}

if (state_with_text.text.join('') !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}




