<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->

List of third-party dependencies grouped by their license type:

<#list licenseMap as e>
<#if e.getValue()?size != 0>
${e.getKey()}
<#list e.getValue() as a>
  ${a.name + ":" + a.version?trim}
</#list>
</#if>
</#list>


Third-party license texts:

-----

Apache License
Version 2.0, January 2004
http://www.apache.org/licenses/

TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

1. Definitions.

"License" shall mean the terms and conditions for use, reproduction,
and distribution as defined by Sections 1 through 9 of this document.

"Licensor" shall mean the copyright owner or entity authorized by
the copyright owner that is granting the License.

"Legal Entity" shall mean the union of the acting entity and all
other entities that control, are controlled by, or are under common
control with that entity. For the purposes of this definition,
"control" means (i) the power, direct or indirect, to cause the
direction or management of such entity, whether by contract or
otherwise, or (ii) ownership of fifty percent (50%) or more of the
outstanding shares, or (iii) beneficial ownership of such entity.

"You" (or "Your") shall mean an individual or Legal Entity
exercising permissions granted by this License.

"Source" form shall mean the preferred form for making modifications,
including but not limited to software source code, documentation
source, and configuration files.

"Object" form shall mean any form resulting from mechanical
transformation or translation of a Source form, including but
not limited to compiled object code, generated documentation,
and conversions to other media types.

"Work" shall mean the work of authorship, whether in Source or
Object form, made available under the License, as indicated by a
copyright notice that is included in or attached to the work
(an example is provided in the Appendix below).

"Derivative Works" shall mean any work, whether in Source or Object
form, that is based on (or derived from) the Work and for which the
editorial revisions, annotations, elaborations, or other modifications
represent, as a whole, an original work of authorship. For the purposes
of this License, Derivative Works shall not include works that remain
separable from, or merely link (or bind by name) to the interfaces of,
the Work and Derivative Works thereof.

"Contribution" shall mean any work of authorship, including
the original version of the Work and any modifications or additions
to that Work or Derivative Works thereof, that is intentionally
submitted to Licensor for inclusion in the Work by the copyright owner
or by an individual or Legal Entity authorized to submit on behalf of
the copyright owner. For the purposes of this definition, "submitted"
means any form of electronic, verbal, or written communication sent
to the Licensor or its representatives, including but not limited to
communication on electronic mailing lists, source code control systems,
and issue tracking systems that are managed by, or on behalf of, the
Licensor for the purpose of discussing and improving the Work, but
excluding communication that is conspicuously marked or otherwise
designated in writing by the copyright owner as "Not a Contribution."

"Contributor" shall mean Licensor and any individual or Legal Entity
on behalf of whom a Contribution has been received by Licensor and
subsequently incorporated within the Work.

2. Grant of Copyright License. Subject to the terms and conditions of
this License, each Contributor hereby grants to You a perpetual,
worldwide, non-exclusive, no-charge, royalty-free, irrevocable
copyright license to reproduce, prepare Derivative Works of,
publicly display, publicly perform, sublicense, and distribute the
Work and such Derivative Works in Source or Object form.

3. Grant of Patent License. Subject to the terms and conditions of
this License, each Contributor hereby grants to You a perpetual,
worldwide, non-exclusive, no-charge, royalty-free, irrevocable
(except as stated in this section) patent license to make, have made,
use, offer to sell, sell, import, and otherwise transfer the Work,
where such license applies only to those patent claims licensable
by such Contributor that are necessarily infringed by their
Contribution(s) alone or by combination of their Contribution(s)
with the Work to which such Contribution(s) was submitted. If You
institute patent litigation against any entity (including a
cross-claim or counterclaim in a lawsuit) alleging that the Work
or a Contribution incorporated within the Work constitutes direct
or contributory patent infringement, then any patent licenses
granted to You under this License for that Work shall terminate
as of the date such litigation is filed.

4. Redistribution. You may reproduce and distribute copies of the
Work or Derivative Works thereof in any medium, with or without
modifications, and in Source or Object form, provided that You
meet the following conditions:

(a) You must give any other recipients of the Work or
Derivative Works a copy of this License; and

(b) You must cause any modified files to carry prominent notices
stating that You changed the files; and

(c) You must retain, in the Source form of any Derivative Works
that You distribute, all copyright, patent, trademark, and
attribution notices from the Source form of the Work,
excluding those notices that do not pertain to any part of
the Derivative Works; and

(d) If the Work includes a "NOTICE" text file as part of its
distribution, then any Derivative Works that You distribute must
include a readable copy of the attribution notices contained
within such NOTICE file, excluding those notices that do not
pertain to any part of the Derivative Works, in at least one
of the following places: within a NOTICE text file distributed
as part of the Derivative Works; within the Source form or
documentation, if provided along with the Derivative Works; or,
within a display generated by the Derivative Works, if and
wherever such third-party notices normally appear. The contents
of the NOTICE file are for informational purposes only and
do not modify the License. You may add Your own attribution
notices within Derivative Works that You distribute, alongside
or as an addendum to the NOTICE text from the Work, provided
that such additional attribution notices cannot be construed
as modifying the License.

You may add Your own copyright statement to Your modifications and
may provide additional or different license terms and conditions
for use, reproduction, or distribution of Your modifications, or
for any such Derivative Works as a whole, provided Your use,
reproduction, and distribution of the Work otherwise complies with
the conditions stated in this License.

5. Submission of Contributions. Unless You explicitly state otherwise,
any Contribution intentionally submitted for inclusion in the Work
by You to the Licensor shall be under the terms and conditions of
this License, without any additional terms or conditions.
Notwithstanding the above, nothing herein shall supersede or modify
the terms of any separate license agreement you may have executed
with Licensor regarding such Contributions.

6. Trademarks. This License does not grant permission to use the trade
names, trademarks, service marks, or product names of the Licensor,
except as required for reasonable and customary use in describing the
origin of the Work and reproducing the content of the NOTICE file.

7. Disclaimer of Warranty. Unless required by applicable law or
agreed to in writing, Licensor provides the Work (and each
Contributor provides its Contributions) on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied, including, without limitation, any warranties or conditions
of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
PARTICULAR PURPOSE. You are solely responsible for determining the
appropriateness of using or redistributing the Work and assume any
risks associated with Your exercise of permissions under this License.

8. Limitation of Liability. In no event and under no legal theory,
whether in tort (including negligence), contract, or otherwise,
unless required by applicable law (such as deliberate and grossly
negligent acts) or agreed to in writing, shall any Contributor be
liable to You for damages, including any direct, indirect, special,
incidental, or consequential damages of any character arising as a
result of this License or out of the use or inability to use the
Work (including but not limited to damages for loss of goodwill,
work stoppage, computer failure or malfunction, or any and all
other commercial damages or losses), even if such Contributor
has been advised of the possibility of such damages.

9. Accepting Warranty or Additional Liability. While redistributing
the Work or Derivative Works thereof, You may choose to offer,
and charge a fee for, acceptance of support, warranty, indemnity,
or other liability obligations and/or rights consistent with this
License. However, in accepting such obligations, You may act only
on Your own behalf and on Your sole responsibility, not on behalf
of any other Contributor, and only if You agree to indemnify,
defend, and hold each Contributor harmless for any liability
incurred by, or claims asserted against, such Contributor by reason
of your accepting any such warranty or additional liability.

END OF TERMS AND CONDITIONS

APPENDIX: How to apply the Apache License to your work.

To apply the Apache License to your work, attach the following
boilerplate notice, with the fields enclosed by brackets "[]"
replaced with your own identifying information. (Don't include
the brackets!)  The text should be enclosed in the appropriate
comment syntax for the file format. We also recommend that a
file or class name and description of purpose be included on the
same "printed page" as the copyright notice for easier
identification within third-party archives.

Copyright [yyyy] [name of copyright owner]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-----

BSD 2-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----

BSD 3-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----

Creative Commons Legal Code

CC0 1.0 Universal

    CREATIVE COMMONS CORPORATION IS NOT A LAW FIRM AND DOES NOT PROVIDE
    LEGAL SERVICES. DISTRIBUTION OF THIS DOCUMENT DOES NOT CREATE AN
    ATTORNEY-CLIENT RELATIONSHIP. CREATIVE COMMONS PROVIDES THIS
    INFORMATION ON AN "AS-IS" BASIS. CREATIVE COMMONS MAKES NO WARRANTIES
    REGARDING THE USE OF THIS DOCUMENT OR THE INFORMATION OR WORKS
    PROVIDED HEREUNDER, AND DISCLAIMS LIABILITY FOR DAMAGES RESULTING FROM
    THE USE OF THIS DOCUMENT OR THE INFORMATION OR WORKS PROVIDED
    HEREUNDER.

Statement of Purpose

The laws of most jurisdictions throughout the world automatically confer
exclusive Copyright and Related Rights (defined below) upon the creator
and subsequent owner(s) (each and all, an "owner") of an original work of
authorship and/or a database (each, a "Work").

Certain owners wish to permanently relinquish those rights to a Work for
the purpose of contributing to a commons of creative, cultural and
scientific works ("Commons") that the public can reliably and without fear
of later claims of infringement build upon, modify, incorporate in other
works, reuse and redistribute as freely as possible in any form whatsoever
and for any purposes, including without limitation commercial purposes.
These owners may contribute to the Commons to promote the ideal of a free
culture and the further production of creative, cultural and scientific
works, or to gain reputation or greater distribution for their Work in
part through the use and efforts of others.

For these and/or other purposes and motivations, and without any
expectation of additional consideration or compensation, the person
associating CC0 with a Work (the "Affirmer"), to the extent that he or she
is an owner of Copyright and Related Rights in the Work, voluntarily
elects to apply CC0 to the Work and publicly distribute the Work under its
terms, with knowledge of his or her Copyright and Related Rights in the
Work and the meaning and intended legal effect of CC0 on those rights.

1. Copyright and Related Rights. A Work made available under CC0 may be
protected by copyright and related or neighboring rights ("Copyright and
Related Rights"). Copyright and Related Rights include, but are not
limited to, the following:

  i. the right to reproduce, adapt, distribute, perform, display,
     communicate, and translate a Work;
 ii. moral rights retained by the original author(s) and/or performer(s);
iii. publicity and privacy rights pertaining to a person's image or
     likeness depicted in a Work;
 iv. rights protecting against unfair competition in regards to a Work,
     subject to the limitations in paragraph 4(a), below;
  v. rights protecting the extraction, dissemination, use and reuse of data
     in a Work;
 vi. database rights (such as those arising under Directive 96/9/EC of the
     European Parliament and of the Council of 11 March 1996 on the legal
     protection of databases, and under any national implementation
     thereof, including any amended or successor version of such
     directive); and
vii. other similar, equivalent or corresponding rights throughout the
     world based on applicable law or treaty, and any national
     implementations thereof.

2. Waiver. To the greatest extent permitted by, but not in contravention
of, applicable law, Affirmer hereby overtly, fully, permanently,
irrevocably and unconditionally waives, abandons, and surrenders all of
Affirmer's Copyright and Related Rights and associated claims and causes
of action, whether now known or unknown (including existing as well as
future claims and causes of action), in the Work (i) in all territories
worldwide, (ii) for the maximum duration provided by applicable law or
treaty (including future time extensions), (iii) in any current or future
medium and for any number of copies, and (iv) for any purpose whatsoever,
including without limitation commercial, advertising or promotional
purposes (the "Waiver"). Affirmer makes the Waiver for the benefit of each
member of the public at large and to the detriment of Affirmer's heirs and
successors, fully intending that such Waiver shall not be subject to
revocation, rescission, cancellation, termination, or any other legal or
equitable action to disrupt the quiet enjoyment of the Work by the public
as contemplated by Affirmer's express Statement of Purpose.

3. Public License Fallback. Should any part of the Waiver for any reason
be judged legally invalid or ineffective under applicable law, then the
Waiver shall be preserved to the maximum extent permitted taking into
account Affirmer's express Statement of Purpose. In addition, to the
extent the Waiver is so judged Affirmer hereby grants to each affected
person a royalty-free, non transferable, non sublicensable, non exclusive,
irrevocable and unconditional license to exercise Affirmer's Copyright and
Related Rights in the Work (i) in all territories worldwide, (ii) for the
maximum duration provided by applicable law or treaty (including future
time extensions), (iii) in any current or future medium and for any number
of copies, and (iv) for any purpose whatsoever, including without
limitation commercial, advertising or promotional purposes (the
"License"). The License shall be deemed effective as of the date CC0 was
applied by Affirmer to the Work. Should any part of the License for any
reason be judged legally invalid or ineffective under applicable law, such
partial invalidity or ineffectiveness shall not invalidate the remainder
of the License, and in such case Affirmer hereby affirms that he or she
will not (i) exercise any of his or her remaining Copyright and Related
Rights in the Work or (ii) assert any associated claims and causes of
action with respect to the Work, in either case contrary to Affirmer's
express Statement of Purpose.

4. Limitations and Disclaimers.

 a. No trademark or patent rights held by Affirmer are waived, abandoned,
    surrendered, licensed or otherwise affected by this document.
 b. Affirmer offers the Work as-is and makes no representations or
    warranties of any kind concerning the Work, express, implied,
    statutory or otherwise, including without limitation warranties of
    title, merchantability, fitness for a particular purpose, non
    infringement, or the absence of latent or other defects, accuracy, or
    the present or absence of errors, whether or not discoverable, all to
    the greatest extent permissible under applicable law.
 c. Affirmer disclaims responsibility for clearing rights of other persons
    that may apply to the Work or any use thereof, including without
    limitation any person's Copyright and Related Rights in the Work.
    Further, Affirmer disclaims responsibility for obtaining any necessary
    consents, permissions or other rights required for any use of the
    Work.
 d. Affirmer understands and acknowledges that Creative Commons is not a
    party to this document and has no duty or obligation with respect to
    this CC0 or use of the Work.

-----

COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0

1. Definitions.

      1.1. "Contributor" means each individual or entity that
      creates or contributes to the creation of Modifications.

      1.2. "Contributor Version" means the combination of the
      Original Software, prior Modifications used by a
      Contributor (if any), and the Modifications made by that
      particular Contributor.

      1.3. "Covered Software" means (a) the Original Software, or
      (b) Modifications, or (c) the combination of files
      containing Original Software with files containing
      Modifications, in each case including portions thereof.

      1.4. "Executable" means the Covered Software in any form
      other than Source Code.

      1.5. "Initial Developer" means the individual or entity
      that first makes Original Software available under this
      License.

      1.6. "Larger Work" means a work which combines Covered
      Software or portions thereof with code not governed by the
      terms of this License.

      1.7. "License" means this document.

      1.8. "Licensable" means having the right to grant, to the
      maximum extent possible, whether at the time of the initial
      grant or subsequently acquired, any and all of the rights
      conveyed herein.

      1.9. "Modifications" means the Source Code and Executable
      form of any of the following:

            A. Any file that results from an addition to,
            deletion from or modification of the contents of a
            file containing Original Software or previous
            Modifications;

            B. Any new file that contains any part of the
            Original Software or previous Modification; or

            C. Any new file that is contributed or otherwise made
            available under the terms of this License.

      1.10. "Original Software" means the Source Code and
      Executable form of computer software code that is
      originally released under this License.

      1.11. "Patent Claims" means any patent claim(s), now owned
      or hereafter acquired, including without limitation,
      method, process, and apparatus claims, in any patent
      Licensable by grantor.

      1.12. "Source Code" means (a) the common form of computer
      software code in which modifications are made and (b)
      associated documentation included in or with such code.

      1.13. "You" (or "Your") means an individual or a legal
      entity exercising rights under, and complying with all of
      the terms of, this License. For legal entities, "You"
      includes any entity which controls, is controlled by, or is
      under common control with You. For purposes of this
      definition, "control" means (a) the power, direct or
      indirect, to cause the direction or management of such
      entity, whether by contract or otherwise, or (b) ownership
      of more than fifty percent (50%) of the outstanding shares
      or beneficial ownership of such entity.

2. License Grants.

      2.1. The Initial Developer Grant.

      Conditioned upon Your compliance with Section 3.1 below and
      subject to third party intellectual property claims, the
      Initial Developer hereby grants You a world-wide,
      royalty-free, non-exclusive license:

            (a) under intellectual property rights (other than
            patent or trademark) Licensable by Initial Developer,
            to use, reproduce, modify, display, perform,
            sublicense and distribute the Original Software (or
            portions thereof), with or without Modifications,
            and/or as part of a Larger Work; and

            (b) under Patent Claims infringed by the making,
            using or selling of Original Software, to make, have
            made, use, practice, sell, and offer for sale, and/or
            otherwise dispose of the Original Software (or
            portions thereof).

            (c) The licenses granted in Sections 2.1(a) and (b)
            are effective on the date Initial Developer first
            distributes or otherwise makes the Original Software
            available to a third party under the terms of this
            License.

            (d) Notwithstanding Section 2.1(b) above, no patent
            license is granted: (1) for code that You delete from
            the Original Software, or (2) for infringements
            caused by: (i) the modification of the Original
            Software, or (ii) the combination of the Original
            Software with other software or devices.

      2.2. Contributor Grant.

      Conditioned upon Your compliance with Section 3.1 below and
      subject to third party intellectual property claims, each
      Contributor hereby grants You a world-wide, royalty-free,
      non-exclusive license:

            (a) under intellectual property rights (other than
            patent or trademark) Licensable by Contributor to
            use, reproduce, modify, display, perform, sublicense
            and distribute the Modifications created by such
            Contributor (or portions thereof), either on an
            unmodified basis, with other Modifications, as
            Covered Software and/or as part of a Larger Work; and


            (b) under Patent Claims infringed by the making,
            using, or selling of Modifications made by that
            Contributor either alone and/or in combination with
            its Contributor Version (or portions of such
            combination), to make, use, sell, offer for sale,
            have made, and/or otherwise dispose of: (1)
            Modifications made by that Contributor (or portions
            thereof); and (2) the combination of Modifications
            made by that Contributor with its Contributor Version
            (or portions of such combination).

            (c) The licenses granted in Sections 2.2(a) and
            2.2(b) are effective on the date Contributor first
            distributes or otherwise makes the Modifications
            available to a third party.

            (d) Notwithstanding Section 2.2(b) above, no patent
            license is granted: (1) for any code that Contributor
            has deleted from the Contributor Version; (2) for
            infringements caused by: (i) third party
            modifications of Contributor Version, or (ii) the
            combination of Modifications made by that Contributor
            with other software (except as part of the
            Contributor Version) or other devices; or (3) under
            Patent Claims infringed by Covered Software in the
            absence of Modifications made by that Contributor.

3. Distribution Obligations.

      3.1. Availability of Source Code.

      Any Covered Software that You distribute or otherwise make
      available in Executable form must also be made available in
      Source Code form and that Source Code form must be
      distributed only under the terms of this License. You must
      include a copy of this License with every copy of the
      Source Code form of the Covered Software You distribute or
      otherwise make available. You must inform recipients of any
      such Covered Software in Executable form as to how they can
      obtain such Covered Software in Source Code form in a
      reasonable manner on or through a medium customarily used
      for software exchange.

      3.2. Modifications.

      The Modifications that You create or to which You
      contribute are governed by the terms of this License. You
      represent that You believe Your Modifications are Your
      original creation(s) and/or You have sufficient rights to
      grant the rights conveyed by this License.

      3.3. Required Notices.

      You must include a notice in each of Your Modifications
      that identifies You as the Contributor of the Modification.
      You may not remove or alter any copyright, patent or
      trademark notices contained within the Covered Software, or
      any notices of licensing or any descriptive text giving
      attribution to any Contributor or the Initial Developer.

      3.4. Application of Additional Terms.

      You may not offer or impose any terms on any Covered
      Software in Source Code form that alters or restricts the
      applicable version of this License or the recipients"
      rights hereunder. You may choose to offer, and to charge a
      fee for, warranty, support, indemnity or liability
      obligations to one or more recipients of Covered Software.
      However, you may do so only on Your own behalf, and not on
      behalf of the Initial Developer or any Contributor. You
      must make it absolutely clear that any such warranty,
      support, indemnity or liability obligation is offered by
      You alone, and You hereby agree to indemnify the Initial
      Developer and every Contributor for any liability incurred
      by the Initial Developer or such Contributor as a result of
      warranty, support, indemnity or liability terms You offer.


      3.5. Distribution of Executable Versions.

      You may distribute the Executable form of the Covered
      Software under the terms of this License or under the terms
      of a license of Your choice, which may contain terms
      different from this License, provided that You are in
      compliance with the terms of this License and that the
      license for the Executable form does not attempt to limit
      or alter the recipient"s rights in the Source Code form
      from the rights set forth in this License. If You
      distribute the Covered Software in Executable form under a
      different license, You must make it absolutely clear that
      any terms which differ from this License are offered by You
      alone, not by the Initial Developer or Contributor. You
      hereby agree to indemnify the Initial Developer and every
      Contributor for any liability incurred by the Initial
      Developer or such Contributor as a result of any such terms
      You offer.

      3.6. Larger Works.

      You may create a Larger Work by combining Covered Software
      with other code not governed by the terms of this License
      and distribute the Larger Work as a single product. In such
      a case, You must make sure the requirements of this License
      are fulfilled for the Covered Software.

4. Versions of the License.

      4.1. New Versions.

      Sun Microsystems, Inc. is the initial license steward and
      may publish revised and/or new versions of this License
      from time to time. Each version will be given a
      distinguishing version number. Except as provided in
      Section 4.3, no one other than the license steward has the
      right to modify this License.

      4.2. Effect of New Versions.

      You may always continue to use, distribute or otherwise
      make the Covered Software available under the terms of the
      version of the License under which You originally received
      the Covered Software. If the Initial Developer includes a
      notice in the Original Software prohibiting it from being
      distributed or otherwise made available under any
      subsequent version of the License, You must distribute and
      make the Covered Software available under the terms of the
      version of the License under which You originally received
      the Covered Software. Otherwise, You may also choose to
      use, distribute or otherwise make the Covered Software
      available under the terms of any subsequent version of the
      License published by the license steward.

      4.3. Modified Versions.

      When You are an Initial Developer and You want to create a
      new license for Your Original Software, You may create and
      use a modified version of this License if You: (a) rename
      the license and remove any references to the name of the
      license steward (except to note that the license differs
      from this License); and (b) otherwise make it clear that
      the license contains terms which differ from this License.


5. DISCLAIMER OF WARRANTY.

COVERED SOFTWARE IS PROVIDED UNDER THIS LICENSE ON AN "AS IS"
BASIS, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED,
INCLUDING, WITHOUT LIMITATION, WARRANTIES THAT THE COVERED
SOFTWARE IS FREE OF DEFECTS, MERCHANTABLE, FIT FOR A PARTICULAR
PURPOSE OR NON-INFRINGING. THE ENTIRE RISK AS TO THE QUALITY AND
PERFORMANCE OF THE COVERED SOFTWARE IS WITH YOU. SHOULD ANY
COVERED SOFTWARE PROVE DEFECTIVE IN ANY RESPECT, YOU (NOT THE
INITIAL DEVELOPER OR ANY OTHER CONTRIBUTOR) ASSUME THE COST OF
ANY NECESSARY SERVICING, REPAIR OR CORRECTION. THIS DISCLAIMER OF
WARRANTY CONSTITUTES AN ESSENTIAL PART OF THIS LICENSE. NO USE OF
ANY COVERED SOFTWARE IS AUTHORIZED HEREUNDER EXCEPT UNDER THIS
DISCLAIMER.

6. TERMINATION.

      6.1. This License and the rights granted hereunder will
      terminate automatically if You fail to comply with terms
      herein and fail to cure such breach within 30 days of
      becoming aware of the breach. Provisions which, by their
      nature, must remain in effect beyond the termination of
      this License shall survive.

      6.2. If You assert a patent infringement claim (excluding
      declaratory judgment actions) against Initial Developer or
      a Contributor (the Initial Developer or Contributor against
      whom You assert such claim is referred to as "Participant")
      alleging that the Participant Software (meaning the
      Contributor Version where the Participant is a Contributor
      or the Original Software where the Participant is the
      Initial Developer) directly or indirectly infringes any
      patent, then any and all rights granted directly or
      indirectly to You by such Participant, the Initial
      Developer (if the Initial Developer is not the Participant)
      and all Contributors under Sections 2.1 and/or 2.2 of this
      License shall, upon 60 days notice from Participant
      terminate prospectively and automatically at the expiration
      of such 60 day notice period, unless if within such 60 day
      period You withdraw Your claim with respect to the
      Participant Software against such Participant either
      unilaterally or pursuant to a written agreement with
      Participant.

      6.3. In the event of termination under Sections 6.1 or 6.2
      above, all end user licenses that have been validly granted
      by You or any distributor hereunder prior to termination
      (excluding licenses granted to You by any distributor)
      shall survive termination.

7. LIMITATION OF LIABILITY.

UNDER NO CIRCUMSTANCES AND UNDER NO LEGAL THEORY, WHETHER TORT
(INCLUDING NEGLIGENCE), CONTRACT, OR OTHERWISE, SHALL YOU, THE
INITIAL DEVELOPER, ANY OTHER CONTRIBUTOR, OR ANY DISTRIBUTOR OF
COVERED SOFTWARE, OR ANY SUPPLIER OF ANY OF SUCH PARTIES, BE
LIABLE TO ANY PERSON FOR ANY INDIRECT, SPECIAL, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES OF ANY CHARACTER INCLUDING, WITHOUT
LIMITATION, DAMAGES FOR LOST PROFITS, LOSS OF GOODWILL, WORK
STOPPAGE, COMPUTER FAILURE OR MALFUNCTION, OR ANY AND ALL OTHER
COMMERCIAL DAMAGES OR LOSSES, EVEN IF SUCH PARTY SHALL HAVE BEEN
INFORMED OF THE POSSIBILITY OF SUCH DAMAGES. THIS LIMITATION OF
LIABILITY SHALL NOT APPLY TO LIABILITY FOR DEATH OR PERSONAL
INJURY RESULTING FROM SUCH PARTY"S NEGLIGENCE TO THE EXTENT
APPLICABLE LAW PROHIBITS SUCH LIMITATION. SOME JURISDICTIONS DO
NOT ALLOW THE EXCLUSION OR LIMITATION OF INCIDENTAL OR
CONSEQUENTIAL DAMAGES, SO THIS EXCLUSION AND LIMITATION MAY NOT
APPLY TO YOU.

8. U.S. GOVERNMENT END USERS.

The Covered Software is a "commercial item," as that term is
defined in 48 C.F.R. 2.101 (Oct. 1995), consisting of "commercial
computer software" (as that term is defined at 48 C.F.R. "
252.227-7014(a)(1)) and "commercial computer software
documentation" as such terms are used in 48 C.F.R. 12.212 (Sept.
1995). Consistent with 48 C.F.R. 12.212 and 48 C.F.R. 227.7202-1
through 227.7202-4 (June 1995), all U.S. Government End Users
acquire Covered Software with only those rights set forth herein.
This U.S. Government Rights clause is in lieu of, and supersedes,
any other FAR, DFAR, or other clause or provision that addresses
Government rights in computer software under this License.

9. MISCELLANEOUS.

This License represents the complete agreement concerning subject
matter hereof. If any provision of this License is held to be
unenforceable, such provision shall be reformed only to the
extent necessary to make it enforceable. This License shall be
governed by the law of the jurisdiction specified in a notice
contained within the Original Software (except to the extent
applicable law, if any, provides otherwise), excluding such
jurisdiction"s conflict-of-law provisions. Any litigation
relating to this License shall be subject to the jurisdiction of
the courts located in the jurisdiction and venue specified in a
notice contained within the Original Software, with the losing
party responsible for costs, including, without limitation, court
costs and reasonable attorneys" fees and expenses. The
application of the United Nations Convention on Contracts for the
International Sale of Goods is expressly excluded. Any law or
regulation which provides that the language of a contract shall
be construed against the drafter shall not apply to this License.
You agree that You alone are responsible for compliance with the
United States export administration regulations (and the export
control laws and regulation of any other countries) when You use,
distribute or otherwise make available any Covered Software.

10. RESPONSIBILITY FOR CLAIMS.

As between Initial Developer and the Contributors, each party is
responsible for claims and damages arising, directly or
indirectly, out of its utilization of rights under this License
and You agree to work with Initial Developer and Contributors to
distribute such responsibility on an equitable basis. Nothing
herein is intended or shall be deemed to constitute any admission
of liability.

-----

COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.1
1. Definitions.

  1.1. "Contributor" means each individual or entity that creates or
  contributes to the creation of Modifications.

  1.2. "Contributor Version" means the combination of the Original
  Software, prior Modifications used by a Contributor (if any), and
  the Modifications made by that particular Contributor.

  1.3. "Covered Software" means (a) the Original Software, or (b)
  Modifications, or (c) the combination of files containing Original
  Software with files containing Modifications, in each case including
  portions thereof.

  1.4. "Executable" means the Covered Software in any form other than
  Source Code.

  1.5. "Initial Developer" means the individual or entity that first
  makes Original Software available under this License.

  1.6. "Larger Work" means a work which combines Covered Software or
  portions thereof with code not governed by the terms of this License.

  1.7. "License" means this document.

  1.8. "Licensable" means having the right to grant, to the maximum
  extent possible, whether at the time of the initial grant or
  subsequently acquired, any and all of the rights conveyed herein.

  1.9. "Modifications" means the Source Code and Executable form of
  any of the following:

  A. Any file that results from an addition to, deletion from or
  modification of the contents of a file containing Original Software
  or previous Modifications;

  B. Any new file that contains any part of the Original Software or
  previous Modification; or

  C. Any new file that is contributed or otherwise made available
  under the terms of this License.

  1.10. "Original Software" means the Source Code and Executable form
  of computer software code that is originally released under this
  License.

  1.11. "Patent Claims" means any patent claim(s), now owned or
  hereafter acquired, including without limitation, method, process,
  and apparatus claims, in any patent Licensable by grantor.

  1.12. "Source Code" means (a) the common form of computer software
  code in which modifications are made and (b) associated
  documentation included in or with such code.

  1.13. "You" (or "Your") means an individual or a legal entity
  exercising rights under, and complying with all of the terms of,
  this License. For legal entities, "You" includes any entity which
  controls, is controlled by, or is under common control with You. For
  purposes of this definition, "control" means (a) the power, direct
  or indirect, to cause the direction or management of such entity,
  whether by contract or otherwise, or (b) ownership of more than
  fifty percent (50%) of the outstanding shares or beneficial
  ownership of such entity.

2. License Grants.

  2.1. The Initial Developer Grant.

  Conditioned upon Your compliance with Section 3.1 below and subject
  to third party intellectual property claims, the Initial Developer
  hereby grants You a world-wide, royalty-free, non-exclusive license:

  (a) under intellectual property rights (other than patent or
  trademark) Licensable by Initial Developer, to use, reproduce,
  modify, display, perform, sublicense and distribute the Original
  Software (or portions thereof), with or without Modifications,
  and/or as part of a Larger Work; and

  (b) under Patent Claims infringed by the making, using or selling of
  Original Software, to make, have made, use, practice, sell, and
  offer for sale, and/or otherwise dispose of the Original Software
  (or portions thereof).

  (c) The licenses granted in Sections 2.1(a) and (b) are effective on
  the date Initial Developer first distributes or otherwise makes the
  Original Software available to a third party under the terms of this
  License.

  (d) Notwithstanding Section 2.1(b) above, no patent license is
  granted: (1) for code that You delete from the Original Software, or
  (2) for infringements caused by: (i) the modification of the
  Original Software, or (ii) the combination of the Original Software
  with other software or devices.

  2.2. Contributor Grant.

  Conditioned upon Your compliance with Section 3.1 below and subject
  to third party intellectual property claims, each Contributor hereby
  grants You a world-wide, royalty-free, non-exclusive license:

  (a) under intellectual property rights (other than patent or
  trademark) Licensable by Contributor to use, reproduce, modify,
  display, perform, sublicense and distribute the Modifications
  created by such Contributor (or portions thereof), either on an
  unmodified basis, with other Modifications, as Covered Software
  and/or as part of a Larger Work; and

  (b) under Patent Claims infringed by the making, using, or selling
  of Modifications made by that Contributor either alone and/or in
  combination with its Contributor Version (or portions of such
  combination), to make, use, sell, offer for sale, have made, and/or
  otherwise dispose of: (1) Modifications made by that Contributor (or
  portions thereof); and (2) the combination of Modifications made by
  that Contributor with its Contributor Version (or portions of such
  combination).

  (c) The licenses granted in Sections 2.2(a) and 2.2(b) are effective
  on the date Contributor first distributes or otherwise makes the
  Modifications available to a third party.

  (d) Notwithstanding Section 2.2(b) above, no patent license is
  granted: (1) for any code that Contributor has deleted from the
  Contributor Version; (2) for infringements caused by: (i) third
  party modifications of Contributor Version, or (ii) the combination
  of Modifications made by that Contributor with other software
  (except as part of the Contributor Version) or other devices; or (3)
  under Patent Claims infringed by Covered Software in the absence of
  Modifications made by that Contributor.

3. Distribution Obligations.

  3.1. Availability of Source Code.

  Any Covered Software that You distribute or otherwise make available
  in Executable form must also be made available in Source Code form
  and that Source Code form must be distributed only under the terms
  of this License. You must include a copy of this License with every
  copy of the Source Code form of the Covered Software You distribute
  or otherwise make available. You must inform recipients of any such
  Covered Software in Executable form as to how they can obtain such
  Covered Software in Source Code form in a reasonable manner on or
  through a medium customarily used for software exchange.

  3.2. Modifications.

  The Modifications that You create or to which You contribute are
  governed by the terms of this License. You represent that You
  believe Your Modifications are Your original creation(s) and/or You
  have sufficient rights to grant the rights conveyed by this License.

  3.3. Required Notices.

  You must include a notice in each of Your Modifications that
  identifies You as the Contributor of the Modification. You may not
  remove or alter any copyright, patent or trademark notices contained
  within the Covered Software, or any notices of licensing or any
  descriptive text giving attribution to any Contributor or the
  Initial Developer.

  3.4. Application of Additional Terms.

  You may not offer or impose any terms on any Covered Software in
  Source Code form that alters or restricts the applicable version of
  this License or the recipients' rights hereunder. You may choose to
  offer, and to charge a fee for, warranty, support, indemnity or
  liability obligations to one or more recipients of Covered Software.
  However, you may do so only on Your own behalf, and not on behalf of
  the Initial Developer or any Contributor. You must make it
  absolutely clear that any such warranty, support, indemnity or
  liability obligation is offered by You alone, and You hereby agree
  to indemnify the Initial Developer and every Contributor for any
  liability incurred by the Initial Developer or such Contributor as a
  result of warranty, support, indemnity or liability terms You offer.

  3.5. Distribution of Executable Versions.

  You may distribute the Executable form of the Covered Software under
  the terms of this License or under the terms of a license of Your
  choice, which may contain terms different from this License,
  provided that You are in compliance with the terms of this License
  and that the license for the Executable form does not attempt to
  limit or alter the recipient's rights in the Source Code form from
  the rights set forth in this License. If You distribute the Covered
  Software in Executable form under a different license, You must make
  it absolutely clear that any terms which differ from this License
  are offered by You alone, not by the Initial Developer or
  Contributor. You hereby agree to indemnify the Initial Developer and
  every Contributor for any liability incurred by the Initial
  Developer or such Contributor as a result of any such terms You offer.

  3.6. Larger Works.

  You may create a Larger Work by combining Covered Software with
  other code not governed by the terms of this License and distribute
  the Larger Work as a single product. In such a case, You must make
  sure the requirements of this License are fulfilled for the Covered
  Software.

4. Versions of the License.

  4.1. New Versions.

  Oracle is the initial license steward and may publish revised and/or
  new versions of this License from time to time. Each version will be
  given a distinguishing version number. Except as provided in Section
  4.3, no one other than the license steward has the right to modify
  this License.

  4.2. Effect of New Versions.

  You may always continue to use, distribute or otherwise make the
  Covered Software available under the terms of the version of the
  License under which You originally received the Covered Software. If
  the Initial Developer includes a notice in the Original Software
  prohibiting it from being distributed or otherwise made available
  under any subsequent version of the License, You must distribute and
  make the Covered Software available under the terms of the version
  of the License under which You originally received the Covered
  Software. Otherwise, You may also choose to use, distribute or
  otherwise make the Covered Software available under the terms of any
  subsequent version of the License published by the license steward.

  4.3. Modified Versions.

  When You are an Initial Developer and You want to create a new
  license for Your Original Software, You may create and use a
  modified version of this License if You: (a) rename the license and
  remove any references to the name of the license steward (except to
  note that the license differs from this License); and (b) otherwise
  make it clear that the license contains terms which differ from this
  License.

5. DISCLAIMER OF WARRANTY.

  COVERED SOFTWARE IS PROVIDED UNDER THIS LICENSE ON AN "AS IS" BASIS,
  WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED,
  INCLUDING, WITHOUT LIMITATION, WARRANTIES THAT THE COVERED SOFTWARE
  IS FREE OF DEFECTS, MERCHANTABLE, FIT FOR A PARTICULAR PURPOSE OR
  NON-INFRINGING. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF
  THE COVERED SOFTWARE IS WITH YOU. SHOULD ANY COVERED SOFTWARE PROVE
  DEFECTIVE IN ANY RESPECT, YOU (NOT THE INITIAL DEVELOPER OR ANY
  OTHER CONTRIBUTOR) ASSUME THE COST OF ANY NECESSARY SERVICING,
  REPAIR OR CORRECTION. THIS DISCLAIMER OF WARRANTY CONSTITUTES AN
  ESSENTIAL PART OF THIS LICENSE. NO USE OF ANY COVERED SOFTWARE IS
  AUTHORIZED HEREUNDER EXCEPT UNDER THIS DISCLAIMER.

6. TERMINATION.

  6.1. This License and the rights granted hereunder will terminate
  automatically if You fail to comply with terms herein and fail to
  cure such breach within 30 days of becoming aware of the breach.
  Provisions which, by their nature, must remain in effect beyond the
  termination of this License shall survive.

  6.2. If You assert a patent infringement claim (excluding
  declaratory judgment actions) against Initial Developer or a
  Contributor (the Initial Developer or Contributor against whom You
  assert such claim is referred to as "Participant") alleging that the
  Participant Software (meaning the Contributor Version where the
  Participant is a Contributor or the Original Software where the
  Participant is the Initial Developer) directly or indirectly
  infringes any patent, then any and all rights granted directly or
  indirectly to You by such Participant, the Initial Developer (if the
  Initial Developer is not the Participant) and all Contributors under
  Sections 2.1 and/or 2.2 of this License shall, upon 60 days notice
  from Participant terminate prospectively and automatically at the
  expiration of such 60 day notice period, unless if within such 60
  day period You withdraw Your claim with respect to the Participant
  Software against such Participant either unilaterally or pursuant to
  a written agreement with Participant.

  6.3. If You assert a patent infringement claim against Participant
  alleging that the Participant Software directly or indirectly
  infringes any patent where such claim is resolved (such as by
  license or settlement) prior to the initiation of patent
  infringement litigation, then the reasonable value of the licenses
  granted by such Participant under Sections 2.1 or 2.2 shall be taken
  into account in determining the amount or value of any payment or
  license.

  6.4. In the event of termination under Sections 6.1 or 6.2 above,
  all end user licenses that have been validly granted by You or any
  distributor hereunder prior to termination (excluding licenses
  granted to You by any distributor) shall survive termination.

7. LIMITATION OF LIABILITY.

  UNDER NO CIRCUMSTANCES AND UNDER NO LEGAL THEORY, WHETHER TORT
  (INCLUDING NEGLIGENCE), CONTRACT, OR OTHERWISE, SHALL YOU, THE
  INITIAL DEVELOPER, ANY OTHER CONTRIBUTOR, OR ANY DISTRIBUTOR OF
  COVERED SOFTWARE, OR ANY SUPPLIER OF ANY OF SUCH PARTIES, BE LIABLE
  TO ANY PERSON FOR ANY INDIRECT, SPECIAL, INCIDENTAL, OR
  CONSEQUENTIAL DAMAGES OF ANY CHARACTER INCLUDING, WITHOUT
  LIMITATION, DAMAGES FOR LOSS OF GOODWILL, WORK STOPPAGE, COMPUTER
  FAILURE OR MALFUNCTION, OR ANY AND ALL OTHER COMMERCIAL DAMAGES OR
  LOSSES, EVEN IF SUCH PARTY SHALL HAVE BEEN INFORMED OF THE
  POSSIBILITY OF SUCH DAMAGES. THIS LIMITATION OF LIABILITY SHALL NOT
  APPLY TO LIABILITY FOR DEATH OR PERSONAL INJURY RESULTING FROM SUCH
  PARTY'S NEGLIGENCE TO THE EXTENT APPLICABLE LAW PROHIBITS SUCH
  LIMITATION. SOME JURISDICTIONS DO NOT ALLOW THE EXCLUSION OR
  LIMITATION OF INCIDENTAL OR CONSEQUENTIAL DAMAGES, SO THIS EXCLUSION
  AND LIMITATION MAY NOT APPLY TO YOU.

8. U.S. GOVERNMENT END USERS.

  The Covered Software is a "commercial item," as that term is defined
  in 48 C.F.R. 2.101 (Oct. 1995), consisting of "commercial computer
  software" (as that term is defined at 48 C.F.R. §
  252.227-7014(a)(1)) and "commercial computer software documentation"
  as such terms are used in 48 C.F.R. 12.212 (Sept. 1995). Consistent
  with 48 C.F.R. 12.212 and 48 C.F.R. 227.7202-1 through 227.7202-4
  (June 1995), all U.S. Government End Users acquire Covered Software
  with only those rights set forth herein. This U.S. Government Rights
  clause is in lieu of, and supersedes, any other FAR, DFAR, or other
  clause or provision that addresses Government rights in computer
  software under this License.

9. MISCELLANEOUS.

  This License represents the complete agreement concerning subject
  matter hereof. If any provision of this License is held to be
  unenforceable, such provision shall be reformed only to the extent
  necessary to make it enforceable. This License shall be governed by
  the law of the jurisdiction specified in a notice contained within
  the Original Software (except to the extent applicable law, if any,
  provides otherwise), excluding such jurisdiction's conflict-of-law
  provisions. Any litigation relating to this License shall be subject
  to the jurisdiction of the courts located in the jurisdiction and
  venue specified in a notice contained within the Original Software,
  with the losing party responsible for costs, including, without
  limitation, court costs and reasonable attorneys' fees and expenses.
  The application of the United Nations Convention on Contracts for
  the International Sale of Goods is expressly excluded. Any law or
  regulation which provides that the language of a contract shall be
  construed against the drafter shall not apply to this License. You
  agree that You alone are responsible for compliance with the United
  States export administration regulations (and the export control
  laws and regulation of any other countries) when You use, distribute
  or otherwise make available any Covered Software.

10. RESPONSIBILITY FOR CLAIMS.

  As between Initial Developer and the Contributors, each party is
  responsible for claims and damages arising, directly or indirectly,
  out of its utilization of rights under this License and You agree to
  work with Initial Developer and Contributors to distribute such
  responsibility on an equitable basis. Nothing herein is intended or
  shall be deemed to constitute any admission of liability.

-----

Eclipse Distribution License - v 1.0

Copyright (c) 2007, Eclipse Foundation, Inc. and its licensors.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.

    Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.

    Neither the name of the Eclipse Foundation, Inc. nor the names of its
    contributors may be used to endorse or promote products derived from this
    software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.

-----

Eclipse Public License - v 2.0

THE ACCOMPANYING PROGRAM IS PROVIDED UNDER THE TERMS OF THIS ECLIPSE
PUBLIC LICENSE ("AGREEMENT"). ANY USE, REPRODUCTION OR DISTRIBUTION OF
THE PROGRAM CONSTITUTES RECIPIENT'S ACCEPTANCE OF THIS AGREEMENT.

1. DEFINITIONS "Contribution" means:

a) in the case of the initial Contributor, the initial content
Distributed under this Agreement, and b) in the case of each subsequent
Contributor: i) changes to the Program, and ii) additions to the
Program; where such changes and/or additions to the Program originate
from and are Distributed by that particular Contributor. A Contribution
"originates" from a Contributor if it was added to the Program by such
Contributor itself or anyone acting on such Contributor's behalf.
Contributions do not include changes or additions to the Program that
are not Modified Works.

"Contributor" means any person or entity that Distributes the Program.

"Licensed Patents" mean patent claims licensable by a Contributor which
are necessarily infringed by the use or sale of its Contribution alone
or when combined with the Program.

"Program" means the Contributions Distributed in accordance with this
Agreement.

"Recipient" means anyone who receives the Program under this Agreement
or any Secondary License (as applicable), including Contributors.

"Derivative Works" shall mean any work, whether in Source Code or other
form, that is based on (or derived from) the Program and for which the
editorial revisions, annotations, elaborations, or other modifications
represent, as a whole, an original work of authorship.

"Modified Works" shall mean any work in Source Code or other form that
results from an addition to, deletion from, or modification of the
contents of the Program, including, for purposes of clarity any new file
in Source Code form that contains any contents of the Program. Modified
Works shall not include works that contain only declarations,
interfaces, types, classes, structures, or files of the Program solely
in each case in order to link to, bind by name, or subclass the Program
or Modified Works thereof.

"Distribute" means the acts of a) distributing or b) making available in
any manner that enables the transfer of a copy.

"Source Code" means the form of a Program preferred for making
modifications, including but not limited to software source code,
documentation source, and configuration files.

"Secondary License" means either the GNU General Public License, Version
2.0, or any later versions of that license, including any exceptions or
additional permissions as identified by the initial Contributor.

2. GRANT OF RIGHTS a) Subject to the terms of this Agreement, each
Contributor hereby grants Recipient a non-exclusive, worldwide,
royalty-free copyright license to reproduce, prepare Derivative Works
of, publicly display, publicly perform, Distribute and sublicense the
Contribution of such Contributor, if any, and such Derivative Works.
b) Subject to the terms of this Agreement, each Contributor hereby
grants Recipient a non-exclusive, worldwide, royalty-free patent
license under Licensed Patents to make, use, sell, offer to sell,
import and otherwise transfer the Contribution of such Contributor,
if any, in Source Code or other form. This patent license shall apply
to the combination of the Contribution and the Program if, at the
time the Contribution is added by the Contributor, such addition of
the Contribution causes such combination to be covered by the
Licensed Patents. The patent license shall not apply to any other
combinations which include the Contribution. No hardware per se is
licensed hereunder. c) Recipient understands that although each
Contributor grants the licenses to its Contributions set forth
herein, no assurances are provided by any Contributor that the
Program does not infringe the patent or other intellectual property
rights of any other entity. Each Contributor disclaims any liability
to Recipient for claims brought by any other entity based on
infringement of intellectual property rights or otherwise. As a
condition to exercising the rights and licenses granted hereunder,
each Recipient hereby assumes sole responsibility to secure any other
intellectual property rights needed, if any. For example, if a third
party patent license is required to allow Recipient to Distribute the
Program, it is Recipient's responsibility to acquire that license
before distributing the Program. d) Each Contributor represents that
to its knowledge it has sufficient copyright rights in its
Contribution, if any, to grant the copyright license set forth in
this Agreement. e) Notwithstanding the terms of any Secondary
License, no Contributor makes additional grants to any Recipient
(other than those set forth in this Agreement) as a result of such
Recipient's receipt of the Program under the terms of a Secondary
License (if permitted under the terms of Section 3).
3. REQUIREMENTS 3.1 If a Contributor Distributes the Program in any
form, then: a) the Program must also be made available as Source
Code, in accordance with section 3.2, and the Contributor must
accompany the Program with a statement that the Source Code for the
Program is available under this Agreement, and informs Recipients how
to obtain it in a reasonable manner on or through a medium
customarily used for software exchange; and b) the Contributor may
Distribute the Program under a license different than this Agreement,
provided that such license: i) effectively disclaims on behalf of all
other Contributors all warranties and conditions, express and
implied, including warranties or conditions of title and
non-infringement, and implied warranties or conditions of
merchantability and fitness for a particular purpose; ii) effectively
excludes on behalf of all other Contributors all liability for
damages, including direct, indirect, special, incidental and
consequential damages, such as lost profits; iii) does not attempt to
limit or alter the recipients' rights in the Source Code under
section 3.2; and iv) requires any subsequent distribution of the
Program by any party to be under a license that satisfies the
requirements of this section 3. 3.2 When the Program is Distributed
as Source Code: a) it must be made available under this Agreement, or
if the Program (i) is combined with other material in a separate file
or files made available under a Secondary License, and (ii) the
initial Contributor attached to the Source Code the notice described
in Exhibit A of this Agreement, then the Program may be made
available under the terms of such Secondary Licenses, and b) a copy
of this Agreement must be included with each copy of the Program. 3.3
Contributors may not remove or alter any copyright, patent,
trademark, attribution notices, disclaimers of warranty, or
limitations of liability ("notices") contained within the Program
from any copy of the Program which they Distribute, provided that
Contributors may add their own appropriate notices.
4. COMMERCIAL DISTRIBUTION Commercial distributors of software may
accept certain responsibilities with respect to end users, business
partners and the like. While this license is intended to facilitate
the commercial use of the Program, the Contributor who includes the
Program in a commercial product offering should do so in a manner
which does not create potential liability for other Contributors.
Therefore, if a Contributor includes the Program in a commercial
product offering, such Contributor ("Commercial Contributor") hereby
agrees to defend and indemnify every other Contributor ("Indemnified
Contributor") against any losses, damages and costs (collectively
"Losses") arising from claims, lawsuits and other legal actions
brought by a third party against the Indemnified Contributor to the
extent caused by the acts or omissions of such Commercial Contributor
in connection with its distribution of the Program in a commercial
product offering. The obligations in this section do not apply to any
claims or Losses relating to any actual or alleged intellectual
property infringement. In order to qualify, an Indemnified
Contributor must: a) promptly notify the Commercial Contributor in
writing of such claim, and b) allow the Commercial Contributor to
control, and cooperate with the Commercial Contributor in, the
defense and any related settlement negotiations. The Indemnified
Contributor may participate in any such claim at its own expense.

For example, a Contributor might include the Program in a commercial
product offering, Product X. That Contributor is then a Commercial
Contributor. If that Commercial Contributor then makes performance
claims, or offers warranties related to Product X, those performance
claims and warranties are such Commercial Contributor's responsibility
alone. Under this section, the Commercial Contributor would have to
defend claims against the other Contributors related to those
performance claims and warranties, and if a court requires any other
Contributor to pay any damages as a result, the Commercial Contributor
must pay those damages.

5. NO WARRANTY EXCEPT AS EXPRESSLY SET FORTH IN THIS AGREEMENT, AND TO
THE EXTENT PERMITTED BY APPLICABLE LAW, THE PROGRAM IS PROVIDED ON AN
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER
EXPRESS OR IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR
CONDITIONS OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR
A PARTICULAR PURPOSE. Each Recipient is solely responsible for
determining the appropriateness of using and distributing the Program
and assumes all risks associated with its exercise of rights under
this Agreement, including but not limited to the risks and costs of
program errors, compliance with applicable laws, damage to or loss of
data, programs or equipment, and unavailability or interruption of
operations.

6. DISCLAIMER OF LIABILITY EXCEPT AS EXPRESSLY SET FORTH IN THIS
AGREEMENT, AND TO THE EXTENT PERMITTED BY APPLICABLE LAW, NEITHER
RECIPIENT NOR ANY CONTRIBUTORS SHALL HAVE ANY LIABILITY FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING WITHOUT LIMITATION LOST PROFITS), HOWEVER CAUSED
AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
WAY OUT OF THE USE OR DISTRIBUTION OF THE PROGRAM OR THE EXERCISE OF
ANY RIGHTS GRANTED HEREUNDER, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

7. GENERAL If any provision of this Agreement is invalid or
unenforceable under applicable law, it shall not affect the validity
or enforceability of the remainder of the terms of this Agreement,
and without further action by the parties hereto, such provision
shall be reformed to the minimum extent necessary to make such
provision valid and enforceable.

If Recipient institutes patent litigation against any entity (including
a cross-claim or counterclaim in a lawsuit) alleging that the Program
itself (excluding combinations of the Program with other software or
hardware) infringes such Recipient's patent(s), then such Recipient's
rights granted under Section 2(b) shall terminate as of the date such
litigation is filed.

All Recipient's rights under this Agreement shall terminate if it fails
to comply with any of the material terms or conditions of this Agreement
and does not cure such failure in a reasonable period of time after
becoming aware of such noncompliance. If all Recipient's rights under
this Agreement terminate, Recipient agrees to cease use and distribution
of the Program as soon as reasonably practicable. However, Recipient's
obligations under this Agreement and any licenses granted by Recipient
relating to the Program shall continue and survive.

Everyone is permitted to copy and distribute copies of this Agreement,
but in order to avoid inconsistency the Agreement is copyrighted and may
only be modified in the following manner. The Agreement Steward reserves
the right to publish new versions (including revisions) of this
Agreement from time to time. No one other than the Agreement Steward has
the right to modify this Agreement. The Eclipse Foundation is the
initial Agreement Steward. The Eclipse Foundation may assign the
responsibility to serve as the Agreement Steward to a suitable separate
entity. Each new version of the Agreement will be given a distinguishing
version number. The Program (including Contributions) may always be
Distributed subject to the version of the Agreement under which it was
received. In addition, after a new version of the Agreement is
published, Contributor may elect to Distribute the Program (including
its Contributions) under the new version.

Except as expressly stated in Sections 2(a) and 2(b) above, Recipient
receives no rights or licenses to the intellectual property of any
Contributor under this Agreement, whether expressly, by implication,
estoppel or otherwise. All rights in the Program not expressly granted
under this Agreement are reserved. Nothing in this Agreement is intended
to be enforceable by any entity that is not a Contributor or Recipient.
No third-party beneficiary rights are created under this Agreement.

Exhibit A - Form of Secondary Licenses Notice "This Source Code may also
be made available under the following Secondary Licenses when the
conditions for such availability set forth in the Eclipse Public
License, v. 2.0 are satisfied: {name license(s), version(s), and
exceptions or additional permissions here}."

Simply including a copy of this Agreement, including this Exhibit A is
not sufficient to license the Source Code under Secondary Licenses.

If it is not possible or desirable to put the notice in a particular
file, then You may include the notice in a location (such as a LICENSE
file in a relevant directory) where a recipient would be likely to look
for such a notice.

You may add additional accurate notices of copyright ownership.

-----

GO LICENSE

Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----

The JSON License
Copyright (c) 2002 JSON.org

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

The Software shall be used for Good, not Evil.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE
AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF
OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

-----

                  GNU LIBRARY GENERAL PUBLIC LICENSE
                       Version 2, June 1991

 Copyright (C) 1991 Free Software Foundation, Inc.
 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 Everyone is permitted to copy and distribute verbatim copies
 of this license document, but changing it is not allowed.

[This is the first released version of the library GPL.  It is
 numbered 2 because it goes with version 2 of the ordinary GPL.]

                            Preamble

  The licenses for most software are designed to take away your
freedom to share and change it.  By contrast, the GNU General Public
Licenses are intended to guarantee your freedom to share and change
free software--to make sure the software is free for all its users.

  This license, the Library General Public License, applies to some
specially designated Free Software Foundation software, and to any
other libraries whose authors decide to use it.  You can use it for
your libraries, too.

  When we speak of free software, we are referring to freedom, not
price.  Our General Public Licenses are designed to make sure that you
have the freedom to distribute copies of free software (and charge for
this service if you wish), that you receive source code or can get it
if you want it, that you can change the software or use pieces of it
in new free programs; and that you know you can do these things.

  To protect your rights, we need to make restrictions that forbid
anyone to deny you these rights or to ask you to surrender the rights.
These restrictions translate to certain responsibilities for you if
you distribute copies of the library, or if you modify it.

  For example, if you distribute copies of the library, whether gratis
or for a fee, you must give the recipients all the rights that we gave
you.  You must make sure that they, too, receive or can get the source
code.  If you link a program with the library, you must provide
complete object files to the recipients so that they can relink them
with the library, after making changes to the library and recompiling
it.  And you must show them these terms so they know their rights.

  Our method of protecting your rights has two steps: (1) copyright
the library, and (2) offer you this license which gives you legal
permission to copy, distribute and/or modify the library.

  Also, for each distributor's protection, we want to make certain
that everyone understands that there is no warranty for this free
library.  If the library is modified by someone else and passed on, we
want its recipients to know that what they have is not the original
version, so that any problems introduced by others will not reflect on
the original authors' reputations.

  Finally, any free program is threatened constantly by software
patents.  We wish to avoid the danger that companies distributing free
software will individually obtain patent licenses, thus in effect
transforming the program into proprietary software.  To prevent this,
we have made it clear that any patent must be licensed for everyone's
free use or not licensed at all.

  Most GNU software, including some libraries, is covered by the ordinary
GNU General Public License, which was designed for utility programs.  This
license, the GNU Library General Public License, applies to certain
designated libraries.  This license is quite different from the ordinary
one; be sure to read it in full, and don't assume that anything in it is
the same as in the ordinary license.

  The reason we have a separate public license for some libraries is that
they blur the distinction we usually make between modifying or adding to a
program and simply using it.  Linking a program with a library, without
changing the library, is in some sense simply using the library, and is
analogous to running a utility program or application program.  However, in
a textual and legal sense, the linked executable is a combined work, a
derivative of the original library, and the ordinary General Public License
treats it as such.

  Because of this blurred distinction, using the ordinary General
Public License for libraries did not effectively promote software
sharing, because most developers did not use the libraries.  We
concluded that weaker conditions might promote sharing better.

  However, unrestricted linking of non-free programs would deprive the
users of those programs of all benefit from the free status of the
libraries themselves.  This Library General Public License is intended to
permit developers of non-free programs to use free libraries, while
preserving your freedom as a user of such programs to change the free
libraries that are incorporated in them.  (We have not seen how to achieve
this as regards changes in header files, but we have achieved it as regards
changes in the actual functions of the Library.)  The hope is that this
will lead to faster development of free libraries.

  The precise terms and conditions for copying, distribution and
modification follow.  Pay close attention to the difference between a
"work based on the library" and a "work that uses the library".  The
former contains code derived from the library, while the latter only
works together with the library.

  Note that it is possible for a library to be covered by the ordinary
General Public License rather than by this special one.

                  GNU LIBRARY GENERAL PUBLIC LICENSE
   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

  0. This License Agreement applies to any software library which
contains a notice placed by the copyright holder or other authorized
party saying it may be distributed under the terms of this Library
General Public License (also called "this License").  Each licensee is
addressed as "you".

  A "library" means a collection of software functions and/or data
prepared so as to be conveniently linked with application programs
(which use some of those functions and data) to form executables.

  The "Library", below, refers to any such software library or work
which has been distributed under these terms.  A "work based on the
Library" means either the Library or any derivative work under
copyright law: that is to say, a work containing the Library or a
portion of it, either verbatim or with modifications and/or translated
straightforwardly into another language.  (Hereinafter, translation is
included without limitation in the term "modification".)

  "Source code" for a work means the preferred form of the work for
making modifications to it.  For a library, complete source code means
all the source code for all modules it contains, plus any associated
interface definition files, plus the scripts used to control compilation
and installation of the library.

  Activities other than copying, distribution and modification are not
covered by this License; they are outside its scope.  The act of
running a program using the Library is not restricted, and output from
such a program is covered only if its contents constitute a work based
on the Library (independent of the use of the Library in a tool for
writing it).  Whether that is true depends on what the Library does
and what the program that uses the Library does.

  1. You may copy and distribute verbatim copies of the Library's
complete source code as you receive it, in any medium, provided that
you conspicuously and appropriately publish on each copy an
appropriate copyright notice and disclaimer of warranty; keep intact
all the notices that refer to this License and to the absence of any
warranty; and distribute a copy of this License along with the
Library.

  You may charge a fee for the physical act of transferring a copy,
and you may at your option offer warranty protection in exchange for a
fee.

  2. You may modify your copy or copies of the Library or any portion
of it, thus forming a work based on the Library, and copy and
distribute such modifications or work under the terms of Section 1
above, provided that you also meet all of these conditions:

    a) The modified work must itself be a software library.

    b) You must cause the files modified to carry prominent notices
    stating that you changed the files and the date of any change.

    c) You must cause the whole of the work to be licensed at no
    charge to all third parties under the terms of this License.

    d) If a facility in the modified Library refers to a function or a
    table of data to be supplied by an application program that uses
    the facility, other than as an argument passed when the facility
    is invoked, then you must make a good faith effort to ensure that,
    in the event an application does not supply such function or
    table, the facility still operates, and performs whatever part of
    its purpose remains meaningful.

    (For example, a function in a library to compute square roots has
    a purpose that is entirely well-defined independent of the
    application.  Therefore, Subsection 2d requires that any
    application-supplied function or table used by this function must
    be optional: if the application does not supply it, the square
    root function must still compute square roots.)

These requirements apply to the modified work as a whole.  If
identifiable sections of that work are not derived from the Library,
and can be reasonably considered independent and separate works in
themselves, then this License, and its terms, do not apply to those
sections when you distribute them as separate works.  But when you
distribute the same sections as part of a whole which is a work based
on the Library, the distribution of the whole must be on the terms of
this License, whose permissions for other licensees extend to the
entire whole, and thus to each and every part regardless of who wrote
it.

Thus, it is not the intent of this section to claim rights or contest
your rights to work written entirely by you; rather, the intent is to
exercise the right to control the distribution of derivative or
collective works based on the Library.

In addition, mere aggregation of another work not based on the Library
with the Library (or with a work based on the Library) on a volume of
a storage or distribution medium does not bring the other work under
the scope of this License.

  3. You may opt to apply the terms of the ordinary GNU General Public
License instead of this License to a given copy of the Library.  To do
this, you must alter all the notices that refer to this License, so
that they refer to the ordinary GNU General Public License, version 2,
instead of to this License.  (If a newer version than version 2 of the
ordinary GNU General Public License has appeared, then you can specify
that version instead if you wish.)  Do not make any other change in
these notices.

  Once this change is made in a given copy, it is irreversible for
that copy, so the ordinary GNU General Public License applies to all
subsequent copies and derivative works made from that copy.

  This option is useful when you wish to copy part of the code of
the Library into a program that is not a library.

  4. You may copy and distribute the Library (or a portion or
derivative of it, under Section 2) in object code or executable form
under the terms of Sections 1 and 2 above provided that you accompany
it with the complete corresponding machine-readable source code, which
must be distributed under the terms of Sections 1 and 2 above on a
medium customarily used for software interchange.

  If distribution of object code is made by offering access to copy
from a designated place, then offering equivalent access to copy the
source code from the same place satisfies the requirement to
distribute the source code, even though third parties are not
compelled to copy the source along with the object code.

  5. A program that contains no derivative of any portion of the
Library, but is designed to work with the Library by being compiled or
linked with it, is called a "work that uses the Library".  Such a
work, in isolation, is not a derivative work of the Library, and
therefore falls outside the scope of this License.

  However, linking a "work that uses the Library" with the Library
creates an executable that is a derivative of the Library (because it
contains portions of the Library), rather than a "work that uses the
library".  The executable is therefore covered by this License.
Section 6 states terms for distribution of such executables.

  When a "work that uses the Library" uses material from a header file
that is part of the Library, the object code for the work may be a
derivative work of the Library even though the source code is not.
Whether this is true is especially significant if the work can be
linked without the Library, or if the work is itself a library.  The
threshold for this to be true is not precisely defined by law.

  If such an object file uses only numerical parameters, data
structure layouts and accessors, and small macros and small inline
functions (ten lines or less in length), then the use of the object
file is unrestricted, regardless of whether it is legally a derivative
work.  (Executables containing this object code plus portions of the
Library will still fall under Section 6.)

  Otherwise, if the work is a derivative of the Library, you may
distribute the object code for the work under the terms of Section 6.
Any executables containing that work also fall under Section 6,
whether or not they are linked directly with the Library itself.

  6. As an exception to the Sections above, you may also compile or
link a "work that uses the Library" with the Library to produce a
work containing portions of the Library, and distribute that work
under terms of your choice, provided that the terms permit
modification of the work for the customer's own use and reverse
engineering for debugging such modifications.

  You must give prominent notice with each copy of the work that the
Library is used in it and that the Library and its use are covered by
this License.  You must supply a copy of this License.  If the work
during execution displays copyright notices, you must include the
copyright notice for the Library among them, as well as a reference
directing the user to the copy of this License.  Also, you must do one
of these things:

    a) Accompany the work with the complete corresponding
    machine-readable source code for the Library including whatever
    changes were used in the work (which must be distributed under
    Sections 1 and 2 above); and, if the work is an executable linked
    with the Library, with the complete machine-readable "work that
    uses the Library", as object code and/or source code, so that the
    user can modify the Library and then relink to produce a modified
    executable containing the modified Library.  (It is understood
    that the user who changes the contents of definitions files in the
    Library will not necessarily be able to recompile the application
    to use the modified definitions.)

    b) Accompany the work with a written offer, valid for at
    least three years, to give the same user the materials
    specified in Subsection 6a, above, for a charge no more
    than the cost of performing this distribution.

    c) If distribution of the work is made by offering access to copy
    from a designated place, offer equivalent access to copy the above
    specified materials from the same place.

    d) Verify that the user has already received a copy of these
    materials or that you have already sent this user a copy.

  For an executable, the required form of the "work that uses the
Library" must include any data and utility programs needed for
reproducing the executable from it.  However, as a special exception,
the source code distributed need not include anything that is normally
distributed (in either source or binary form) with the major
components (compiler, kernel, and so on) of the operating system on
which the executable runs, unless that component itself accompanies
the executable.

  It may happen that this requirement contradicts the license
restrictions of other proprietary libraries that do not normally
accompany the operating system.  Such a contradiction means you cannot
use both them and the Library together in an executable that you
distribute.

  7. You may place library facilities that are a work based on the
Library side-by-side in a single library together with other library
facilities not covered by this License, and distribute such a combined
library, provided that the separate distribution of the work based on
the Library and of the other library facilities is otherwise
permitted, and provided that you do these two things:

    a) Accompany the combined library with a copy of the same work
    based on the Library, uncombined with any other library
    facilities.  This must be distributed under the terms of the
    Sections above.

    b) Give prominent notice with the combined library of the fact
    that part of it is a work based on the Library, and explaining
    where to find the accompanying uncombined form of the same work.

  8. You may not copy, modify, sublicense, link with, or distribute
the Library except as expressly provided under this License.  Any
attempt otherwise to copy, modify, sublicense, link with, or
distribute the Library is void, and will automatically terminate your
rights under this License.  However, parties who have received copies,
or rights, from you under this License will not have their licenses
terminated so long as such parties remain in full compliance.

  9. You are not required to accept this License, since you have not
signed it.  However, nothing else grants you permission to modify or
distribute the Library or its derivative works.  These actions are
prohibited by law if you do not accept this License.  Therefore, by
modifying or distributing the Library (or any work based on the
Library), you indicate your acceptance of this License to do so, and
all its terms and conditions for copying, distributing or modifying
the Library or works based on it.

  10. Each time you redistribute the Library (or any work based on the
Library), the recipient automatically receives a license from the
original licensor to copy, distribute, link with or modify the Library
subject to these terms and conditions.  You may not impose any further
restrictions on the recipients' exercise of the rights granted herein.
You are not responsible for enforcing compliance by third parties to
this License.

  11. If, as a consequence of a court judgment or allegation of patent
infringement or for any other reason (not limited to patent issues),
conditions are imposed on you (whether by court order, agreement or
otherwise) that contradict the conditions of this License, they do not
excuse you from the conditions of this License.  If you cannot
distribute so as to satisfy simultaneously your obligations under this
License and any other pertinent obligations, then as a consequence you
may not distribute the Library at all.  For example, if a patent
license would not permit royalty-free redistribution of the Library by
all those who receive copies directly or indirectly through you, then
the only way you could satisfy both it and this License would be to
refrain entirely from distribution of the Library.

If any portion of this section is held invalid or unenforceable under any
particular circumstance, the balance of the section is intended to apply,
and the section as a whole is intended to apply in other circumstances.

It is not the purpose of this section to induce you to infringe any
patents or other property right claims or to contest validity of any
such claims; this section has the sole purpose of protecting the
integrity of the free software distribution system which is
implemented by public license practices.  Many people have made
generous contributions to the wide range of software distributed
through that system in reliance on consistent application of that
system; it is up to the author/donor to decide if he or she is willing
to distribute software through any other system and a licensee cannot
impose that choice.

This section is intended to make thoroughly clear what is believed to
be a consequence of the rest of this License.

  12. If the distribution and/or use of the Library is restricted in
certain countries either by patents or by copyrighted interfaces, the
original copyright holder who places the Library under this License may add
an explicit geographical distribution limitation excluding those countries,
so that distribution is permitted only in or among countries not thus
excluded.  In such case, this License incorporates the limitation as if
written in the body of this License.

  13. The Free Software Foundation may publish revised and/or new
versions of the Library General Public License from time to time.
Such new versions will be similar in spirit to the present version,
but may differ in detail to address new problems or concerns.

Each version is given a distinguishing version number.  If the Library
specifies a version number of this License which applies to it and
"any later version", you have the option of following the terms and
conditions either of that version or of any later version published by
the Free Software Foundation.  If the Library does not specify a
license version number, you may choose any version ever published by
the Free Software Foundation.

  14. If you wish to incorporate parts of the Library into other free
programs whose distribution conditions are incompatible with these,
write to the author to ask for permission.  For software which is
copyrighted by the Free Software Foundation, write to the Free
Software Foundation; we sometimes make exceptions for this.  Our
decision will be guided by the two goals of preserving the free status
of all derivatives of our free software and of promoting the sharing
and reuse of software generally.

                            NO WARRANTY

  15. BECAUSE THE LIBRARY IS LICENSED FREE OF CHARGE, THERE IS NO
WARRANTY FOR THE LIBRARY, TO THE EXTENT PERMITTED BY APPLICABLE LAW.
EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR
OTHER PARTIES PROVIDE THE LIBRARY "AS IS" WITHOUT WARRANTY OF ANY
KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE.  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE
LIBRARY IS WITH YOU.  SHOULD THE LIBRARY PROVE DEFECTIVE, YOU ASSUME
THE COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION.

  16. IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN
WRITING WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY
AND/OR REDISTRIBUTE THE LIBRARY AS PERMITTED ABOVE, BE LIABLE TO YOU
FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL OR
CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE THE
LIBRARY (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE LIBRARY TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
DAMAGES.

                     END OF TERMS AND CONDITIONS

           How to Apply These Terms to Your New Libraries

  If you develop a new library, and you want it to be of the greatest
possible use to the public, we recommend making it free software that
everyone can redistribute and change.  You can do so by permitting
redistribution under these terms (or, alternatively, under the terms of the
ordinary General Public License).

  To apply these terms, attach the following notices to the library.  It is
safest to attach them to the start of each source file to most effectively
convey the exclusion of warranty; and each file should have at least the
"copyright" line and a pointer to where the full notice is found.

    <one line to give the library's name and a brief idea of what it does.>
    Copyright (C) <year>  <name of author>

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Library General Public
    License as published by the Free Software Foundation; either
    version 2 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Library General Public License for more details.

    You should have received a copy of the GNU Library General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

Also add information on how to contact you by electronic and paper mail.

You should also get your employer (if you work as a programmer) or your
school, if any, to sign a "copyright disclaimer" for the library, if
necessary.  Here is a sample; alter the names:

  Yoyodyne, Inc., hereby disclaims all copyright interest in the
  library `Frob' (a library for tweaking knobs) written by James Random Hacker.

  <signature of Ty Coon>, 1 April 1990
  Ty Coon, President of Vice

That's all there is to it!

-----

The MIT License

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

-----

                    GNU GENERAL PUBLIC LICENSE
                       Version 2, June 1991

 Copyright (C) 1989, 1991 Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 Everyone is permitted to copy and distribute verbatim copies
 of this license document, but changing it is not allowed.

                            Preamble

  The licenses for most software are designed to take away your
freedom to share and change it.  By contrast, the GNU General Public
License is intended to guarantee your freedom to share and change free
software--to make sure the software is free for all its users.  This
General Public License applies to most of the Free Software
Foundation's software and to any other program whose authors commit to
using it.  (Some other Free Software Foundation software is covered by
the GNU Lesser General Public License instead.)  You can apply it to
your programs, too.

  When we speak of free software, we are referring to freedom, not
price.  Our General Public Licenses are designed to make sure that you
have the freedom to distribute copies of free software (and charge for
this service if you wish), that you receive source code or can get it
if you want it, that you can change the software or use pieces of it
in new free programs; and that you know you can do these things.

  To protect your rights, we need to make restrictions that forbid
anyone to deny you these rights or to ask you to surrender the rights.
These restrictions translate to certain responsibilities for you if you
distribute copies of the software, or if you modify it.

  For example, if you distribute copies of such a program, whether
gratis or for a fee, you must give the recipients all the rights that
you have.  You must make sure that they, too, receive or can get the
source code.  And you must show them these terms so they know their
rights.

  We protect your rights with two steps: (1) copyright the software, and
(2) offer you this license which gives you legal permission to copy,
distribute and/or modify the software.

  Also, for each author's protection and ours, we want to make certain
that everyone understands that there is no warranty for this free
software.  If the software is modified by someone else and passed on, we
want its recipients to know that what they have is not the original, so
that any problems introduced by others will not reflect on the original
authors' reputations.

  Finally, any free program is threatened constantly by software
patents.  We wish to avoid the danger that redistributors of a free
program will individually obtain patent licenses, in effect making the
program proprietary.  To prevent this, we have made it clear that any
patent must be licensed for everyone's free use or not licensed at all.

  The precise terms and conditions for copying, distribution and
modification follow.

                    GNU GENERAL PUBLIC LICENSE
   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

  0. This License applies to any program or other work which contains
a notice placed by the copyright holder saying it may be distributed
under the terms of this General Public License.  The "Program", below,
refers to any such program or work, and a "work based on the Program"
means either the Program or any derivative work under copyright law:
that is to say, a work containing the Program or a portion of it,
either verbatim or with modifications and/or translated into another
language.  (Hereinafter, translation is included without limitation in
the term "modification".)  Each licensee is addressed as "you".

Activities other than copying, distribution and modification are not
covered by this License; they are outside its scope.  The act of
running the Program is not restricted, and the output from the Program
is covered only if its contents constitute a work based on the
Program (independent of having been made by running the Program).
Whether that is true depends on what the Program does.

  1. You may copy and distribute verbatim copies of the Program's
source code as you receive it, in any medium, provided that you
conspicuously and appropriately publish on each copy an appropriate
copyright notice and disclaimer of warranty; keep intact all the
notices that refer to this License and to the absence of any warranty;
and give any other recipients of the Program a copy of this License
along with the Program.

You may charge a fee for the physical act of transferring a copy, and
you may at your option offer warranty protection in exchange for a fee.

  2. You may modify your copy or copies of the Program or any portion
of it, thus forming a work based on the Program, and copy and
distribute such modifications or work under the terms of Section 1
above, provided that you also meet all of these conditions:

    a) You must cause the modified files to carry prominent notices
    stating that you changed the files and the date of any change.

    b) You must cause any work that you distribute or publish, that in
    whole or in part contains or is derived from the Program or any
    part thereof, to be licensed as a whole at no charge to all third
    parties under the terms of this License.

    c) If the modified program normally reads commands interactively
    when run, you must cause it, when started running for such
    interactive use in the most ordinary way, to print or display an
    announcement including an appropriate copyright notice and a
    notice that there is no warranty (or else, saying that you provide
    a warranty) and that users may redistribute the program under
    these conditions, and telling the user how to view a copy of this
    License.  (Exception: if the Program itself is interactive but
    does not normally print such an announcement, your work based on
    the Program is not required to print an announcement.)

These requirements apply to the modified work as a whole.  If
identifiable sections of that work are not derived from the Program,
and can be reasonably considered independent and separate works in
themselves, then this License, and its terms, do not apply to those
sections when you distribute them as separate works.  But when you
distribute the same sections as part of a whole which is a work based
on the Program, the distribution of the whole must be on the terms of
this License, whose permissions for other licensees extend to the
entire whole, and thus to each and every part regardless of who wrote it.

Thus, it is not the intent of this section to claim rights or contest
your rights to work written entirely by you; rather, the intent is to
exercise the right to control the distribution of derivative or
collective works based on the Program.

In addition, mere aggregation of another work not based on the Program
with the Program (or with a work based on the Program) on a volume of
a storage or distribution medium does not bring the other work under
the scope of this License.

  3. You may copy and distribute the Program (or a work based on it,
under Section 2) in object code or executable form under the terms of
Sections 1 and 2 above provided that you also do one of the following:

    a) Accompany it with the complete corresponding machine-readable
    source code, which must be distributed under the terms of Sections
    1 and 2 above on a medium customarily used for software interchange; or,

    b) Accompany it with a written offer, valid for at least three
    years, to give any third party, for a charge no more than your
    cost of physically performing source distribution, a complete
    machine-readable copy of the corresponding source code, to be
    distributed under the terms of Sections 1 and 2 above on a medium
    customarily used for software interchange; or,

    c) Accompany it with the information you received as to the offer
    to distribute corresponding source code.  (This alternative is
    allowed only for noncommercial distribution and only if you
    received the program in object code or executable form with such
    an offer, in accord with Subsection b above.)

The source code for a work means the preferred form of the work for
making modifications to it.  For an executable work, complete source
code means all the source code for all modules it contains, plus any
associated interface definition files, plus the scripts used to
control compilation and installation of the executable.  However, as a
special exception, the source code distributed need not include
anything that is normally distributed (in either source or binary
form) with the major components (compiler, kernel, and so on) of the
operating system on which the executable runs, unless that component
itself accompanies the executable.

If distribution of executable or object code is made by offering
access to copy from a designated place, then offering equivalent
access to copy the source code from the same place counts as
distribution of the source code, even though third parties are not
compelled to copy the source along with the object code.

  4. You may not copy, modify, sublicense, or distribute the Program
except as expressly provided under this License.  Any attempt
otherwise to copy, modify, sublicense or distribute the Program is
void, and will automatically terminate your rights under this License.
However, parties who have received copies, or rights, from you under
this License will not have their licenses terminated so long as such
parties remain in full compliance.

  5. You are not required to accept this License, since you have not
signed it.  However, nothing else grants you permission to modify or
distribute the Program or its derivative works.  These actions are
prohibited by law if you do not accept this License.  Therefore, by
modifying or distributing the Program (or any work based on the
Program), you indicate your acceptance of this License to do so, and
all its terms and conditions for copying, distributing or modifying
the Program or works based on it.

  6. Each time you redistribute the Program (or any work based on the
Program), the recipient automatically receives a license from the
original licensor to copy, distribute or modify the Program subject to
these terms and conditions.  You may not impose any further
restrictions on the recipients' exercise of the rights granted herein.
You are not responsible for enforcing compliance by third parties to
this License.

  7. If, as a consequence of a court judgment or allegation of patent
infringement or for any other reason (not limited to patent issues),
conditions are imposed on you (whether by court order, agreement or
otherwise) that contradict the conditions of this License, they do not
excuse you from the conditions of this License.  If you cannot
distribute so as to satisfy simultaneously your obligations under this
License and any other pertinent obligations, then as a consequence you
may not distribute the Program at all.  For example, if a patent
license would not permit royalty-free redistribution of the Program by
all those who receive copies directly or indirectly through you, then
the only way you could satisfy both it and this License would be to
refrain entirely from distribution of the Program.

If any portion of this section is held invalid or unenforceable under
any particular circumstance, the balance of the section is intended to
apply and the section as a whole is intended to apply in other
circumstances.

It is not the purpose of this section to induce you to infringe any
patents or other property right claims or to contest validity of any
such claims; this section has the sole purpose of protecting the
integrity of the free software distribution system, which is
implemented by public license practices.  Many people have made
generous contributions to the wide range of software distributed
through that system in reliance on consistent application of that
system; it is up to the author/donor to decide if he or she is willing
to distribute software through any other system and a licensee cannot
impose that choice.

This section is intended to make thoroughly clear what is believed to
be a consequence of the rest of this License.

  8. If the distribution and/or use of the Program is restricted in
certain countries either by patents or by copyrighted interfaces, the
original copyright holder who places the Program under this License
may add an explicit geographical distribution limitation excluding
those countries, so that distribution is permitted only in or among
countries not thus excluded.  In such case, this License incorporates
the limitation as if written in the body of this License.

  9. The Free Software Foundation may publish revised and/or new versions
of the General Public License from time to time.  Such new versions will
be similar in spirit to the present version, but may differ in detail to
address new problems or concerns.

Each version is given a distinguishing version number.  If the Program
specifies a version number of this License which applies to it and "any
later version", you have the option of following the terms and conditions
either of that version or of any later version published by the Free
Software Foundation.  If the Program does not specify a version number of
this License, you may choose any version ever published by the Free Software
Foundation.

  10. If you wish to incorporate parts of the Program into other free
programs whose distribution conditions are different, write to the author
to ask for permission.  For software which is copyrighted by the Free
Software Foundation, write to the Free Software Foundation; we sometimes
make exceptions for this.  Our decision will be guided by the two goals
of preserving the free status of all derivatives of our free software and
of promoting the sharing and reuse of software generally.

                            NO WARRANTY

  11. BECAUSE THE PROGRAM IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW.  EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE PROGRAM "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED
OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.  THE ENTIRE RISK AS
TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU.  SHOULD THE
PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING,
REPAIR OR CORRECTION.

  12. IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE PROGRAM AS PERMITTED ABOVE, BE LIABLE TO YOU FOR DAMAGES,
INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL OR CONSEQUENTIAL DAMAGES ARISING
OUT OF THE USE OR INABILITY TO USE THE PROGRAM (INCLUDING BUT NOT LIMITED
TO LOSS OF DATA OR DATA BEING RENDERED INACCURATE OR LOSSES SUSTAINED BY
YOU OR THIRD PARTIES OR A FAILURE OF THE PROGRAM TO OPERATE WITH ANY OTHER
PROGRAMS), EVEN IF SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGES.

                     END OF TERMS AND CONDITIONS

            How to Apply These Terms to Your New Programs

  If you develop a new program, and you want it to be of the greatest
possible use to the public, the best way to achieve this is to make it
free software which everyone can redistribute and change under these terms.

  To do so, attach the following notices to the program.  It is safest
to attach them to the start of each source file to most effectively
convey the exclusion of warranty; and each file should have at least
the "copyright" line and a pointer to where the full notice is found.

    <one line to give the program's name and a brief idea of what it does.>
    Copyright (C) <year>  <name of author>

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

Also add information on how to contact you by electronic and paper mail.

If the program is interactive, make it output a short notice like this
when it starts in an interactive mode:

    Gnomovision version 69, Copyright (C) year name of author
    Gnomovision comes with ABSOLUTELY NO WARRANTY; for details type `show w'.
    This is free software, and you are welcome to redistribute it
    under certain conditions; type `show c' for details.

The hypothetical commands `show w' and `show c' should show the appropriate
parts of the General Public License.  Of course, the commands you use may
be called something other than `show w' and `show c'; they could even be
mouse-clicks or menu items--whatever suits your program.

You should also get your employer (if you work as a programmer) or your
school, if any, to sign a "copyright disclaimer" for the program, if
necessary.  Here is a sample; alter the names:

  Yoyodyne, Inc., hereby disclaims all copyright interest in the program
  `Gnomovision' (which makes passes at compilers) written by James Hacker.

  <signature of Ty Coon>, 1 April 1989
  Ty Coon, President of Vice

This General Public License does not permit incorporating your program into
proprietary programs.  If your program is a subroutine library, you may
consider it more useful to permit linking proprietary applications with the
library.  If this is what you want to do, use the GNU Lesser General
Public License instead of this License.



The Universal FOSS Exception, Version 1.0

In addition to the rights set forth in the other license(s) included
in the distribution for this software, data, and/or documentation
(collectively the "Software," and such licenses collectively with this
additional permission the "Software License"), the copyright holders
wish to facilitate interoperability with other software, data, and/or
documentation distributed with complete corresponding source under
a license that is OSI-approved and/or categorized by the FSF as free
(collectively "Other FOSS").  We therefore hereby grant the following
additional permission with respect to the use and distribution of the Software
with Other FOSS, and the constants, function signatures, data structures and
other invocation methods used to run or interact with each of them (as
to each, such software's "Interfaces"):

(i) The Software's Interfaces may, to the extent permitted by the license
of the Other FOSS, be copied into, used and distributed in the Other FOSS
in order to enable interoperability, without requiring a change to the license
of the Other FOSS other than as to any Interfaces of the Software embedded therein.
The Software's Interfaces remain at all times under the Software License,
including without limitation as used in the Other FOSS (which upon any such
use also then contains a portion of the Software under the Software License).

(ii) The Other FOSS's Interfaces may, to the extent permitted by the license
of the Other FOSS, be copied into, used and distributed in the Software in order
to enable interoperability, without requiring that such Interfaces be licensed
under the terms of the Software License or otherwise altering their original terms,
if this does not require any portion of the Software other than such Interfaces
to be licensed under the terms other than the Software License.

(iii) If only Interfaces and no other code is copied between the Software and
the Other FOSS in either direction, the use and/or distribution of the Software
with the Other FOSS shall not be deemed to require that the Other FOSS be licensed
under the license of the Software, other than as to any Interfaces of the Software
copied into the Other FOSS.  This includes, by way of example and without limitation,
statically or dynamically linking the Software together with Other FOSS after
enabling interoperability using the Interfaces of one or both, and distributing
the resulting combination under different licenses for the respective portions thereof.

For avoidance of doubt, a license which is OSI-approved or categorized by the FSF
as free, includes, for the purpose of this permission, such licenses with additional
permissions, and any license that has previously been so-approved or categorized
as free, even if now deprecated or otherwise no longer recognized as approved or free.
Nothing in this additional permission grants any right to distribute any portion of
the Software on terms other than those of the Software License or grants any additional
permission of any kind for use or distribution of the Software in conjunction with
software other than Other FOSS.
