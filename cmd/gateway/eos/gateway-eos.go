/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 * Gateway for the EOS storage backend  
 *
 */

package eos

import (
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
)

const (
	eosBackend = "eos"
)

func init() {
	const eosGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  Path to EOS mount point.

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of minimum 3 characters in length.
     MINIO_SECRET_KEY: Password or secret key of minimum 8 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  EOS:
     EOSLOGLEVEL: 0..n 0=off, 1=errors only, 2=errors/info, 3+ = "debug"
     EOS: url to eos
     EOSUSER: eos username
     EOSUID: eos user uid
     EOSGID: eos user gid
     EOSSTAGE: local fast disk to stage multipart uploads
     EOSREADONLY: true/false
     EOSREADMETHOD: webdav/xrootd/xrdcp (DEFAULT: webdav)
     EOSSLEEP: int ms sleep 1000ms = 1s (default 100 ms)
     VOLUME_PATH: path on eos
     HOOKSURL: url to s3 hooks (not setting this will disable hooks)
     SCRIPTS: path to xroot script
     EOSVALIDBUCKETS: true/false (DEFAULT: true)

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).

EXAMPLES:
  1. Start minio gateway server for EOS backend.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export EOS=url to eos
     $ export EOSLOGLEVEL=1
     $ export EOSUSER=eos username
     $ export EOSUID=eos user uid
     $ export EOSGID=eos user gid
     $ export EOSSTAGE=local fast disk to stage multipart uploads
     $ export VOLUME_PATH=path on eos
     $ export HOOKSURL=url to s3 hooks
     $ export SCRIPTS=path to xroot script
     $ {{.HelpName}} ${VOLUME_PATH}

  2. Start minio gateway server for EOS with edge caching enabled.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export EOS=url to eos
     $ export EOSLOGLEVEL=1
     $ export EOSUSER=eos username
     $ export EOSUID=eos user uid
     $ export EOSGID=eos user gid
     $ export EOSSTAGE=local fast disk to stage multipart uploads
     $ export VOLUME_PATH=path on eos
     $ export HOOKSURL=url to s3 hooks
     $ export SCRIPTS=path to xroot script
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_MAXUSE=80
     $ {{.HelpName}} ${VOLUME_PATH}
`
	minio.RegisterGatewayCommand(cli.Command{
		Name:               eosBackend,
		Usage:              "AARNet's CERN's EOS",
		Action:             eosGatewayMain,
		CustomHelpTemplate: eosGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway eos' command line.
func eosGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, eosBackend, 1)
	}

	minio.StartGateway(ctx, &EOS{ctx.Args().First()})
}
