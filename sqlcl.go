package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"

	"github.com/puellanivis/breton/lib/glog"
	flag "github.com/puellanivis/breton/lib/gnuflag"
	"github.com/puellanivis/breton/lib/os/process"
)

// Version information ready for build-time injection.
var (
	Version    = "v0.0.1"
	Buildstamp = "dev"
)

var protoRegex = regexp.MustCompile(`^[a-z]*\(([^)]+)\)$`)

func DoMySQL(ctx context.Context, dsn string) error {
	fmt.Printf("dsn: %q\n", dsn)

	bin, err := exec.LookPath("mysql")
	if err != nil {
		return err
	}

	uri, err := url.Parse(dsn)
	if err != nil {
		return err
	}

	if uri.User == nil {
		return fmt.Errorf("no user information specified: %#v", uri)
	}

	if matches := protoRegex.FindStringSubmatch(uri.Host); len(matches) > 1 {
		uri.Host = matches[1]
	}

	cmdline := []string{
		bin,
		fmt.Sprintf("--host=%s", uri.Hostname()),
	}

	if port := uri.Port(); port != "" {
		cmdline = append(cmdline, fmt.Sprintf("--port=%s", port))
	}

	if user := uri.User.Username(); user != "" {
		cmdline = append(cmdline, fmt.Sprintf("--user=%s", user))
	}

	if pw, ok := uri.User.Password(); ok {
		cmdline = append(cmdline, fmt.Sprintf("--password=%s", pw))
	}

	cmdline = append(cmdline, strings.TrimPrefix(uri.Path, "/"))

	fmt.Printf("exec %q\n", cmdline)

	return syscall.Exec(cmdline[0], cmdline, os.Environ())
}

func main() {
	flag.Set("logtostderr", "true")

	ctx, done := process.Init("sql-client", Version, Buildstamp)
	defer done()

	args := flag.Args()
	if len(args) < 1 {
		glog.Fatal("you must specify a table")
	}

	dsn, args := args[0], args[1:]

	switch {
	case strings.HasPrefix(dsn, "mysql:"):
		if err := DoMySQL(ctx, dsn); err != nil {
			glog.Fatal(err)
		}

	default:
		if err := DoMySQL(ctx, "mysql://"+dsn); err != nil {
			glog.Fatal(err)
		}
	}
}
