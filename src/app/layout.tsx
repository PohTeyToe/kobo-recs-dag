import type { Metadata } from "next";
import "./globals.css";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Better-than-Quiz — Kobo Recs DAG Demo",
  description:
    "Open Library + dbt + Airflow recommendations pipeline — submitted with Abdallah Safi's Rakuten Kobo Data Engineer Co-op application.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className="bg-stone-50 text-stone-900 antialiased min-h-screen">
        <header className="border-b border-stone-200 bg-white">
          <nav className="max-w-5xl mx-auto px-6 py-4 flex flex-wrap items-center gap-6 text-sm">
            <Link href="/" className="font-semibold tracking-tight">
              kobo-recs-dag
            </Link>
            <Link href="/dag" className="hover:underline">
              Pipeline
            </Link>
            <Link href="/recs" className="hover:underline">
              Recs explorer
            </Link>
            <Link href="/quality" className="hover:underline">
              Data quality
            </Link>
            <span className="ml-auto text-xs text-stone-500">
              Portfolio demo · Open Library data · synthetic events
            </span>
          </nav>
        </header>
        <main className="max-w-5xl mx-auto px-6 py-10">{children}</main>
        <footer className="max-w-5xl mx-auto px-6 py-10 text-xs text-stone-500 border-t border-stone-200 mt-16">
          Built by{" "}
          <a className="underline" href="https://abdallah-safi.vercel.app">
            Abdallah Safi
          </a>{" "}
          · source on{" "}
          <a className="underline" href="https://github.com/PohTeyToe/kobo-recs-dag">
            github.com/PohTeyToe/kobo-recs-dag
          </a>{" "}
          · synthetic event data is for demonstration only and does not represent
          real Kobo telemetry.
        </footer>
      </body>
    </html>
  );
}
