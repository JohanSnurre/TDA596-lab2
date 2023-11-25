// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rename contains the implementation of the 'gorename' command
// whose main function is in golang.org/x/tools/cmd/gorename.
// See the Usage constant for the command documentation.
package rename // import "golang.org/x/tools/refactor/rename"

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	exec "golang.org/x/sys/execabs"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/loader"
	"golang.org/x/tools/go/types/typeutil"
	"golang.org/x/tools/refactor/importgraph"
	"golang.org/x/tools/refactor/satisfy"
)

const Usage = `gorename: precise type-safe renaming of identifiers in Go source code.

Usage:

 gorename (-from <spec> | -offset <file>:#<byte-offset>) -to <name> [-force]

You must specify the object (named entity) to rename using the -offset
or -from flag.  Exactly one must be specified.

Flags:

-offset    specifies the filename and byte offset of an identifier to rename.
           This form is intended for use by text editors.

-from      specifies the object to rename using a query notation;
           This form is intended for interactive use at the command line.
           A legal -from query has one of the following forms:

  "encoding/json".Decoder.Decode        method of package-level named type
  (*"encoding/json".Decoder).Decode     ditto, alternative syntax
  "encoding/json".Decoder.buf           field of package-level named struct type
  "encoding/json".HTMLEscape            package member (const, func, var, type)
  "encoding/json".Decoder.Decode::x     local object x within a method
  "encoding/json".HTMLEscape::x         local object x within a function
  "encoding/json"::x                    object x anywhere within a package
  json.go::x                            object x within file json.go

           Double-quotes must be escaped when writing a shell command.
           Quotes may be omitted for single-segment import paths such as "fmt".

           For methods, the parens and '*' on the receiver type are both
           optional.

           It is an error if one of the ::x queries matches multiple
           objects.

-to        the new name.

-force     causes the renaming to proceed even if conflicts were reported.
           The resulting program may be ill-formed, or experience a change
           in behaviour.

           WARNING: this flag may even cause the renaming tool to crash.
           (In due course this bug will be fixed by moving certain
           analyses into the type-checker.)

-d         display diffs instead of rewriting files

-v         enables verbose logging.

gorename automatically computes the set of packages that might be
affected.  For a local renaming, this is just the package specified by
-from or -offset, but for a potentially exported name, gorename scans
the workspace ($GOROOT and $GOPATH).

gorename rejects renamings of concrete methods that would change the
assignability relation between types and interfaces. If the interface
change was intentional, initiate the renaming at the interface method.

gorename rejects any renaming that would create a conflict at the point
of declaration, or a reference conflict (ambiguity or shadowing), or
anything else that could cause the resulting program not to compile.


Examples:

$ gorename -offset file.go:#123 -to foo

  Rename the object whose identifier is at byte offset 123 within file file.go.

$ gorename -from '"bytes".Buffer.Len' -to Size

  Rename the "Len" method of the *bytes.Buffer type to "Size".
`

// ---- TODO ----

// Correctness:
// - handle dot imports correctly
// - document limitations (reflection, 'implements' algorithm).
// - sketch a proof of exhaustiveness.

// Features:
// - support running on packages specified as *.go files on the command line
// - support running on programs containing errors (loader.Config.AllowErrors)
// - allow users to specify a scope other than "global" (to avoid being
//   stuck by neglected packages in $GOPATH that don't build).
// - support renaming the package clause (no object)
// - support renaming an import path (no ident or object)
//   (requires filesystem + SCM updates).
// - detect and reject edits to autogenerated files (cgo, protobufs)
//   and optionally $GOROOT packages.
// - report all conflicts, or at least all qualitatively distinct ones.
//   Sometimes we stop to avoid redundancy, but
//   it may give a disproportionate sense of safety in -force mode.
// - support renaming all instances of a pattern, e.g.
//   all receiver vars of a given type,
//   all local variables of a given type,
//   all PkgNames for a given package.
// - emit JSON output for other editors and tools.

var (
	// Force enables patching of the source files even if conflicts were reported.
	// The resulting program may be ill-formed.
	// It may even cause gorename to crash.  TODO(adonovan): fix that.
	Force bool

	// Diff causes the tool to display diffs instead of rewriting files.
	Diff bool

	// DiffCmd specifies the diff command used by the -d feature.
	// (The command must accept a -u flag and two filename arguments.)
	DiffCmd = "diff"

	// ConflictError is returned by Main when it aborts the renaming due to conflicts.
	// (It is distinguished because the interesting errors are the conflicts themselves.)
	ConflictError = errors.New("renaming aborted due to conflicts")

	// Verbose enables extra logging.
	Verbose bool
)

var stdout io.Writer = os.Stdout

type renamer struct {
	iprog              *loader.Program
	objsToUpdate       map[types.Object]bool
	hadConflicts       bool
	from, to           string
	satisfyConstraints map[satisfy.Constraint]bool
	packages           map[*types.Package]*loader.PackageInfo // subset of iprog.AllPackages to inspect
	msets              typeutil.MethodSetCache
	changeMethods      bool
}

var reportError = func(posn token.Position, message string) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", posn, message)
}

// importName renames imports of fromPath within the package specified by info.
// If fromName is not empty, importName renames only imports as fromName.
// If the renaming would lead to a conflict, the file is left unchanged.
func importName(iprog *loader.Program, info *loader.PackageInfo, fromPath, fromName, to string) error {
	if fromName == to {
		return nil // no-op (e.g. rename x/foo to y/foo)
	}
	for _, f := range info.Files {
		var from types.Object
		for _, imp := range f.Imports {
			importPath, _ := strconv.Unquote(imp.Path.Value)
			importName := path.Base(importPath)
			if imp.Name != nil {
				importName = imp.Name.Name
			}
			if importPath == fromPath && (fromName == "" || importName == fromName) {
				from = info.Implicits[imp]
				break
			}
		}
		if from == nil {
			continue
		}
		r := renamer{
			iprog:        iprog,
			objsToUpdate: make(map[types.Object]bool),
			to:           to,
			packages:     map[*types.Package]*loader.PackageInfo{info.Pkg: info},
		}
		r.check(from)
		if r.hadConflicts {
			reportError(iprog.Fset.Position(f.Imports[0].Pos()),
				"skipping update of this file")
			continue // ignore errors; leave the existing name
		}
		if err := r.update(); err != nil {
			return err
		}
	}
	return nil
}

func Main(ctxt *build.Context, offsetFlag, fromFlag, to string) error {
	// -- Parse the -from or -offset specifier ----------------------------

	if (offsetFlag == "") == (fromFlag == "") {
		return fmt.Errorf("exactly one of the -from and -offset flags must be specified")
	}

	if !isValidIdentifier(to) {
		return fmt.Errorf("-to %q: not a valid identifier", to)
	}

	if Diff {
		defer func(saved func(string, []byte) error) { writeFile = saved }(writeFile)
		writeFile = diff
	}

	var spec *spec
	var err error
	if fromFlag != "" {
		spec, err = parseFromFlag(ctxt, fromFlag)
	} else {
		spec, err = parseOffsetFlag(ctxt, offsetFlag)
	}
	if err != nil {
		return err
	}

	if spec.fromName == to {
		return fmt.Errorf("the old and new names are the same: %s", to)
	}

	// -- Load the program consisting of the initial package  -------------

	iprog, err := loadProgram(ctxt, map[string]bool{spec.pkg: true})
	if err != nil {
		return err
	}

	fromObjects, err := findFromObjects(iprog, spec)
	if err != nil {
		return err
	}

	// -- Load a larger program, for global renamings ---------------------

	if requiresGlobalRename(fromObjects, to) {
		// For a local refactoring, we needn't load more
		// packages, but if the renaming affects the package's
		// API, we we must load all packages that depend on the
		// package defining the object, plus their tests.

		if Verbose {
			log.Print("Potentially global renaming; scanning workspace...")
		}

		// Scan the workspace and build the import graph.
		_, rev, errors := importgraph.Build(ctxt)
		if len(errors) > 0 {
			// With a large GOPATH tree, errors are inevitable.
			// Report them but proceed.
			fmt.Fprintf(os.Stderr, "While scanning Go workspace:\n")
			for path, err := range errors {
				fmt.Fprintf(os.Stderr, "Package %q: %s.\n", path, err)
			}
		}

		// Enumerate the set of potentially affected packages.
		affectedPackages := make(map[string]bool)
		for _, obj := range fromObjects {
			// External test packages are never imported,
			// so they will never appear in the graph.
			for path := range rev.Search(obj.Pkg().Path()) {
				affectedPackages[path] = true
			}
		}

		// TODO(adonovan): allow the user to specify the scope,
		// or -ignore patterns?  Computing the scope when we
		// don't (yet) support inputs containing errors can make
		// the tool rather brittle.

		// Re-load the larger program.
		iprog, err = loadProgram(ctxt, affectedPackages)
		if err != nil {
			return err
		}

		fromObjects, err = findFromObjects(iprog, spec)
		if err != nil {
			return err
		}
	}

	// -- Do the renaming -------------------------------------------------

	r := renamer{
		iprog:        iprog,
		objsToUpdate: make(map[types.Object]bool),
		from:         spec.fromName,
		to:           to,
		packages:     make(map[*types.Package]*loader.PackageInfo),
	}

	// A renaming initiated at an interface method indicates the
	// intention to rename abstract and concrete methods as needed
	// to preserve assignability.
	for _, obj := range fromObjects {
		if obj, ok := obj.(*types.Func); ok {
			recv := obj.Type().(*types.Signature).Recv()
			if recv != nil && isInterface(recv.Type().Underlying()) {
				r.changeMethods = true
				break
			}
		}
	}

	// Only the initially imported packages (iprog.Imported) and
	// their external tests (iprog.Created) should be inspected or
	// modified, as only they have type-checked functions bodies.
	// The rest are just dependencies, needed only for package-level
	// type information.
	for _, info := range iprog.Imported {
		r.packages[info.Pkg] = info
	}
	for _, info := range iprog.Created { // (tests)
		r.packages[info.Pkg] = info
	}

	for _, from := range fromObjects {
		r.check(from)
	}
	if r.hadConflicts && !Force {
		return ConflictError
	}
	return r.update()
}

// loadProgram loads the specified set of packages (plus their tests)
// and all their dependencies, from source, through the specified build
// context.  Only packages in pkgs will have their functions bodies typechecked.
func loadProgram(ctxt *build.Context, pkgs map[string]bool) (*loader.Program, error) {
	conf := loader.Config{
		Build:      ctxt,
		ParserMode: parser.ParseComments,

		// TODO(adonovan): enable this.  Requires making a lot of code more robust!
		AllowErrors: false,
	}
	// Optimization: don't type-check the bodies of functions in our
	// dependencies, since we only need exported package members.
	conf.TypeCheckFuncBodies = func(p string) bool {
		return pkgs[p] || pkgs[strings.TrimSuffix(p, "_test")]
	}

	if Verbose {
		var list []string
		for pkg := range pkgs {
			list = append(list, pkg)
		}
		sort.Strings(list)
		for _, pkg := range list {
			log.Printf("Loading package: %s", pkg)
		}
	}

	for pkg := range pkgs {
		conf.ImportWithTests(pkg)
	}

	// Ideally we would just return conf.Load() here, but go/types
	// reports certain "soft" errors that gc does not (Go issue 14596).
	// As a workaround, we set AllowErrors=true and then duplicate
	// the loader's error checking but allow soft errors.
	// It would be nice if the loader API permitted "AllowErrors: soft".
	conf.AllowErrors = true
	prog, err := conf.Load()
	if err != nil {
		return nil, err
	}

	var errpkgs []string
	// Report hard errors in indirectly imported packages.
	for _, info := range prog.AllPackages {
		if containsHardErrors(info.Errors) {
			errpkgs = append(errpkgs, info.Pkg.Path())
		}
	}
	if errpkgs != nil {
		var more string
		if len(errpkgs) > 3 {
			more = fmt.Sprintf(" and %d more", len(errpkgs)-3)
			errpkgs = errpkgs[:3]
		}
		return nil, fmt.Errorf("couldn't load packages due to errors: %s%s",
			strings.Join(errpkgs, ", "), more)
	}
	return prog, nil
}

func containsHardErrors(errors []error) bool {
	for _, err := range errors {
		if err, ok := err.(types.Error); ok && err.Soft {
			continue
		}
		return true
	}
	return false
}

// requiresGlobalRename reports whether this renaming could potentially
// affect other packages in the Go workspace.
func requiresGlobalRename(fromObjects []types.Object, to string) bool {
	var tfm bool
	for _, from := range fromObjects {
		if from.Exported() {
			return true
		}
		switch objectKind(from) {
		case "type", "field", "method":
			tfm = true
		}
	}
	if ast.IsExported(to) && tfm {
		// A global renaming may be necessary even if we're
		// exporting a previous unexported name, since if it's
		// the name of a type, field or method, this could
		// change selections in other packages.
		// (We include "type" in this list because a type
		// used as an embedded struct field entails a field
		// renaming.)
		return true
	}
	return false
}

// update updates the input files.
func (r *renamer) update() error {
	// We use token.File, not filename, since a file may appear to
	// belong to multiple packages and be parsed more than once.
	// token.File captures this distinction; filename does not.

	var nidents int
	var filesToUpdate = make(map[*token.File]bool)
	docRegexp := regexp.MustCompile(`\b` + r.from + `\b`)
	for _, info := range r.packages {
		// Mutate the ASTs and note the filenames.
		for id, obj := range info.Defs {
			if r.objsToUpdate[obj] {
				nidents++
				id.Name = r.to
				filesToUpdate[r.iprog.Fset.File(id.Pos())] = true
				// Perform the rename in doc comments too.
				if doc := r.docComment(id); doc != nil {
					for _, comment := range doc.List {
						comment.Text = docRegexp.ReplaceAllString(comment.Text, r.to)
					}
				}
			}
		}

		for id, obj := range info.Uses {
			if r.objsToUpdate[obj] {
				nidents++
				id.Name = r.to
				filesToUpdate[r.iprog.Fset.File(id.Pos())] = true
			}
		}
	}

	// Renaming not supported if cgo files are affected.
	var generatedFileNames []string
	for _, info := range r.packages {
		for _, f := range info.Files {
			tokenFile := r.iprog.Fset.File(f.Pos())
			if filesToUpdate[tokenFile] && generated(f, tokenFile) {
				generatedFileNames = append(generatedFileNames, tokenFile.Name())
			}
		}
	}
	if !Force && len(generatedFileNames) > 0 {
		return fmt.Errorf("refusing to modify generated file%s containing DO NOT EDIT marker: %v", plural(len(generatedFileNames)), generatedFileNames)
	}

	// Write affected files.
	var nerrs, npkgs int
	for _, info := range r.packages {
		first := true
		for _, f := range info.Files {
			tokenFile := r.iprog.Fset.File(f.Pos())
			if filesToUpdate[tokenFile] {
				if first {
					npkgs++
					first = false
					if Verbose {
						log.Printf("Updating package %s", info.Pkg.Path())
					}
				}

				filename := tokenFile.Name()
				var buf bytes.Buffer
				if err := format.Node(&buf, r.iprog.Fset, f); err != nil {
					log.Printf("failed to pretty-print syntax tree: %v", err)
					nerrs++
					continue
				}
				if err := writeFile(filename, buf.Bytes()); err != nil {
					log.Print(err)
					nerrs++
				}
			}
		}
	}
	if !Diff {
		fmt.Printf("Renamed %d occurrence%s in %d file%s in %d package%s.\n",
			nidents, plural(nidents),
			len(filesToUpdate), plural(len(filesToUpdate)),
			npkgs, plural(npkgs))
	}
	if nerrs > 0 {
		return fmt.Errorf("failed to rewrite %d file%s", nerrs, plural(nerrs))
	}
	return nil
}

// docComment returns the doc for an identifier.
func (r *renamer) docComment(id *ast.Ident) *ast.CommentGroup {
	_, nodes, _ := r.iprog.PathEnclosingInterval(id.Pos(), id.End())
	for _, node := range nodes {
		switch decl := node.(type) {
		case *ast.FuncDecl:
			return decl.Doc
		case *ast.Field:
			return decl.Doc
		case *ast.GenDecl:
			return decl.Doc
		// For {Type,Value}Spec, if the doc on the spec is absent,
		// search for the enclosing GenDecl
		case *ast.TypeSpec:
			if decl.Doc != nil {
				return decl.Doc
			}
		case *ast.ValueSpec:
			if decl.Doc != nil {
				return decl.Doc
			}
		case *ast.Ident:
		default:
			return nil
		}
	}
	return nil
}

func plural(n int) string {
	if n != 1 {
		return "s"
	}
	return ""
}

// writeFile is a seam for testing and for the -d flag.
var writeFile = reallyWriteFile

func reallyWriteFile(filename string, content []byte) error {
	return os.WriteFile(filename, content, 0644)
}

func diff(filename string, content []byte) error {
	renamed := fmt.Sprintf("%s.%d.renamed", filename, os.Getpid())
	if err := os.WriteFile(renamed, content, 0644); err != nil {
		return err
	}
	defer os.Remove(renamed)

	diff, err := exec.Command(DiffCmd, "-u", filename, renamed).CombinedOutput()
	if len(diff) > 0 {
		// diff exits with a non-zero status when the files don't match.
		// Ignore that failure as long as we get output.
		stdout.Write(diff)
		return nil
	}
	if err != nil {
		return fmt.Errorf("computing diff: %v", err)
	}
	return nil
}
