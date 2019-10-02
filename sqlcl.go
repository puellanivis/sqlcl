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
	Version    = "v0.0.2"
	Buildstamp = "dev"
)

var (
	tcpRegex  = regexp.MustCompile(`tcp\(([^)]+)\)`)
	unixRegex = regexp.MustCompile(`unix\(([^)]+)\)`)
)

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

	cmdline := []string{
		bin,
		fmt.Sprintf("--host=%s", uri.Hostname()),
	}

	return doMySQL(ctx, uri, cmdline)
}

func DoMySQLSocket(ctx context.Context, sock, dsn string) error {
	fmt.Printf("unix_socket: %q\n", sock)
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

	cmdline := []string{
		bin,
		fmt.Sprintf("--socket=%s", sock),
	}

	return doMySQL(ctx, uri, cmdline)
}

func doMySQL(ctx context.Context, uri *url.URL, cmdline []string) error {
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

func DoPostgreSQL(ctx context.Context, dsn string) error {
	fmt.Printf("dsn: %q\n", dsn)

	bin, err := exec.LookPath("psql")
	if err != nil {
		return err
	}

	cmdline := []string{
		bin,
		dsn,
	}

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

	case strings.HasPrefix(dsn, "postgres:"):
		if err := DoPostgreSQL(ctx, dsn); err != nil {
			glog.Fatal(err)
		}

	default:
		if matches := tcpRegex.FindStringSubmatch(dsn); len(matches) > 1 {
			dsn := tcpRegex.ReplaceAllString(dsn, matches[1])

			if err := DoMySQL(ctx, "mysql://"+dsn); err != nil {
				glog.Fatal(err)
			}

			return
		}

		if matches := unixRegex.FindStringSubmatch(dsn); len(matches) > 1 {
			sock := matches[1]
			dsn := unixRegex.ReplaceAllString(dsn, "")

			if err := DoMySQLSocket(ctx, sock, "mysql://"+dsn); err != nil {
				glog.Fatal(err)
			}
		}

		if err := DoMySQL(ctx, "mysql://"+dsn); err != nil {
			glog.Fatal(err)
		}
	}
}
