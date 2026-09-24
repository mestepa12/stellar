import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, forkJoin, map, catchError, throwError, of } from 'rxjs';
import { GitCommit } from '../models/commit.model';

@Injectable({ providedIn: 'root' })
export class GithubService {
  private http = inject(HttpClient);
  private api = 'https://api.github.com';
  private headers = new HttpHeaders({ Accept: 'application/vnd.github.v3+json' });

  // Acepta "owner/repo" o la URL tal cual se copia del navegador (con / final
  // o .git), que es lo que la gente pega.
  parseRepo(input: string): { owner: string; name: string } | null {
    const clean = input.trim().replace(/\/$/, '').replace(/\.git$/, '');
    const fromUrl = clean.match(/github\.com\/([^/\s]+)\/([^/\s]+)/);
    if (fromUrl) return { owner: fromUrl[1], name: fromUrl[2] };
    const fromSlash = clean.match(/^([^/\s]+)\/([^/\s]+)$/);
    if (fromSlash) return { owner: fromSlash[1], name: fromSlash[2] };
    return null;
  }

  // Como mucho 200 commits (2 páginas en paralelo): sin token, la API de GitHub
  // permite 60 peticiones/hora por IP. Si la página 2 falla se pinta con la 1
  // en vez de perderlo todo; un error en la 1 sí se propaga (404, 403).
  getCommits(owner: string, name: string): Observable<GitCommit[]> {
    const opts = { headers: this.headers };
    const url = (page: number) =>
      `${this.api}/repos/${owner}/${name}/commits?per_page=100&page=${page}`;

    const page1$ = this.http.get<any[]>(url(1), opts);
    const page2$ = this.http.get<any[]>(url(2), opts).pipe(catchError(() => of([])));

    return forkJoin([page1$, page2$]).pipe(
      map(([p1, p2]) => [...p1, ...p2].map(c => this.toCommit(c))),
      catchError(err => throwError(() => err))
    );
  }

  private toCommit(c: any): GitCommit {
    return {
      sha: c.sha,
      shortSha: c.sha.slice(0, 7),
      message: c.commit.message.split('\n')[0].slice(0, 80),
      author: c.commit.author?.name ?? 'Unknown',
      authorLogin: c.author?.login ?? null,
      avatarUrl: c.author?.avatar_url ?? null,
      date: new Date(c.commit.author?.date ?? c.commit.committer?.date),
      url: c.html_url,
    };
  }
}
